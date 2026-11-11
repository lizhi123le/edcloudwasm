// 直接抽取 _worker.js 中的 manualPipe / createBufferedTcpWriter / createAsyncMicrotaskQueue，
// 在 Node 里用合成字节流验证：数据完整性、背压生效、done 处理、队列水位。
// 运行：node test/manual-pipe.mjs
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, '..', '_worker.js'), 'utf8');

const slice = (startMark, endMark) => {
    const start = source.indexOf(startMark);
    if (start === -1) throw new Error(`missing ${startMark}`);
    const end = source.indexOf(endMark, start);
    if (end === -1) throw new Error(`missing end ${endMark}`);
    return source.slice(start, end);
};

const manualPipeSrc = slice('const manualPipe = async', '\nconst createBufferedTcpWriter');
const bufferedWriterSrc = slice('const createBufferedTcpWriter =', '\nconst createAsyncMicrotaskQueue');
const queueSrc = slice('const createAsyncMicrotaskQueue =', '\nconst handleSession');

const makeManualPipe = (maxChunkLen) => new Function(
    'bufferSize', 'maxChunkLen', 'flushTime', 'startThreshold', 'smallChunkThreshold',
    `${manualPipeSrc}\nreturn manualPipe;`
)(256 * 1024, maxChunkLen, 3, 50 * 1024 * 1024, 4096);

const uploadSrc = slice('const uploadReadSize', '\nconst handleXwebPost');
const makeBufferedWriter = new Function(`${bufferedWriterSrc}\nreturn createBufferedTcpWriter;`)();
const makeQueue = new Function(`${queueSrc}\nreturn createAsyncMicrotaskQueue;`)();
const makeManualUpload = (handleSession) => new Function('handleSession', `${uploadSrc}\nreturn manualUpload;`)(handleSession);

const earlyDataSrc = slice('const parseEarlyData =', '\nconst handleWebSocketConn');
const parseEarlyData = new Function(`${earlyDataSrc}\nreturn parseEarlyData;`)();

let failures = 0;
const check = (name, ok, detail = '') => {
    console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  ' + detail : ''}`);
    if (!ok) failures++;
};

// 字节序列源：第 i 个字节固定为 i & 0xff，方便校验顺序与重复。
function makeSource(totalBytes, chunkSize, delayMs = 0) {
    let produced = 0;
    return new ReadableStream({
        type: 'bytes',
        async pull(controller) {
            if (produced >= totalBytes) return controller.close();
            if (delayMs) await new Promise(r => setTimeout(r, delayMs));
            const len = Math.min(chunkSize, totalBytes - produced);
            const buf = new Uint8Array(len);
            for (let i = 0; i < len; i++) buf[i] = (produced + i) & 0xff;
            produced += len;
            controller.enqueue(buf);
            // Node 27 alpha 中，close 需要在最后一次 enqueue 的同一次 pull 里调用。
            if (produced >= totalBytes) controller.close();
        }
    });
}

function verify(name, chunks, totalBytes) {
    const all = new Uint8Array(chunks.reduce((n, c) => n + c.byteLength, 0));
    let off = 0;
    for (const c of chunks) all.set(c, (off += c.byteLength) - c.byteLength);
    let ok = all.byteLength === totalBytes;
    for (let i = 0; ok && i < all.byteLength; i++) ok = all[i] === (i & 0xff);
    check(`${name}: 字节完整且顺序正确`, ok, `got=${all.byteLength} expect=${totalBytes} chunks=${chunks.length}`);
    return all;
}

// 1. 同步 send：行为应与改造前一致（合批、完整）
{
    const manualPipe = makeManualPipe(4 * 1024);
    const out = [];
    const total = 1200 * 3000;
    await manualPipe(makeSource(total, 3000), {send(chunk) { out.push(chunk.slice()); }}, () => {});
    verify('同步 send 3KB×1200', out, total);
    check('同步 send 有合批（不是每块一次 send）', out.length < 1200, `sends=${out.length}`);
}

// 2. 异步 send：背压必须串行化，且 send 期间缓冲区不能被复用（延迟后再拷贝）
{
    const manualPipe = makeManualPipe(4 * 1024);
    const out = [];
    let inflight = 0, maxInflight = 0;
    const total = 1000 * 3000;
    await manualPipe(makeSource(total, 3000), {
        async send(chunk) {
            inflight++, maxInflight = Math.max(maxInflight, inflight);
            await new Promise(r => setTimeout(r, 1));
            out.push(chunk.slice());  // 故意在 await 之后才拷贝
            inflight--;
        }
    }, () => {});
    verify('异步 send 3KB×1000', out, total);
    check('异步 send 无并发复用（maxInflight=1）', maxInflight === 1, `maxInflight=${maxInflight}`);
}

// 3. 上游关闭时 done 且 value 为 undefined：不能抛错、最后一块必须刷出
{
    const manualPipe = makeManualPipe(4 * 1024);
    const out = [];
    let closed = false;
    const total = 777;
    await manualPipe(makeSource(total, 777), {send(chunk) { out.push(chunk.slice()); }}, () => {closed = true;});
    verify('done 时最后一块不丢', out, total);
    check('done 不触发 close', closed === false);
}

// 4. 大视图下读到大块：确认 1 次读能拿到上游给的大块（view 上限不是瓶颈）
{
    const manualPipe = makeManualPipe(64 * 1024);
    const out = [];
    const total = 64 * 1024 * 4;
    let reads = 0;
    const src = new ReadableStream({
        type: 'bytes',
        pull(controller) {
            reads++;
            if (reads > 4) return controller.close();
            const buf = new Uint8Array(64 * 1024);
            for (let i = 0; i < buf.length; i++) buf[i] = ((reads - 1) * buf.length + i) & 0xff;
            controller.enqueue(buf);
            if (reads === 4) controller.close();
        }
    });
    await manualPipe(src, {send(chunk) { out.push(chunk.slice()); }}, () => {});
    verify('64KB 上游 + 64KB 视图', out, total);
}

// 5. createAsyncMicrotaskQueue：超过低水位返回 Promise，且队列不越界
{
    let consumed = 0;
    let closed = false;
    const queue = makeQueue(async () => {
        consumed++;
        await new Promise(r => setTimeout(r, 2));
    }, () => {closed = true;}, 4, 1);
    let awaited = 0;
    const t0 = Date.now();
    for (let i = 0; i < 20; i++) {
        const p = queue(new Uint8Array([i]));
        if (p) { awaited++; await p; }
    }
    const elapsed = Date.now() - t0;
    await new Promise(r => setTimeout(r, 40));
    check('队列消费完全部条目', consumed === 20, `consumed=${consumed}`);
    check('超过低水位时确实返回 Promise（背压生效）', awaited > 0, `awaited=${awaited}`);
    check('队列未触发 close（未越界）', closed === false);
    check('背压确实让生产方等待（不能瞬间灌完）', elapsed >= 19 * 2 - 5, `elapsed=${elapsed}ms`);
}

// 6. createBufferedTcpWriter：写入顺序正确、背压生效、不丢数据
{
    const written = [];
    let pendingWrites = 0, maxPending = 0;
    const mockWriter = {
        write(chunk) {
            const copy = chunk.slice();  // 模拟 sink 同步拷贝
            pendingWrites++, maxPending = Math.max(maxPending, pendingWrites);
            return new Promise(r => setTimeout(() => { written.push(copy); pendingWrites--; r(); }, 1));
        }
    };
    let closed = false;
    const write = makeBufferedWriter(mockWriter, () => {closed = true;});
    const payload = new Uint8Array(100000);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    for (let off = 0; off < payload.length; off += 4000) await write(payload.subarray(off, off + 4000));
    // Windows 定时器粒度约 15.6ms，setTimeout(flush, 1) 实际可能十几毫秒，固定 sleep 30ms 会偶发失败。
    const deadline = Date.now() + 2000;
    while (written.reduce((n, c) => n + c.byteLength, 0) < payload.byteLength && Date.now() < deadline) {
        await new Promise(r => setTimeout(r, 5));
    }
    const all = new Uint8Array(written.reduce((n, c) => n + c.byteLength, 0));
    let p = 0;
    for (const c of written) all.set(c, (p += c.byteLength) - c.byteLength);
    let ok = all.byteLength === payload.byteLength;
    for (let i = 0; ok && i < all.length; i++) ok = all[i] === payload[i];
    check('bufferedTcpWriter 数据完整', ok, `got=${all.byteLength} expect=${payload.byteLength} writes=${written.length}`);
    check('bufferedTcpWriter 写串行', maxPending === 1, `maxPending=${maxPending}`);
    check('bufferedTcpWriter 未误关连接', closed === false);
}

// 7. 大块（>= 32KB）直写不越界
{
    const written = [];
    const mockWriter = {write(chunk) { written.push(chunk.slice()); }};
    const write = makeBufferedWriter(mockWriter, () => {});
    const big = new Uint8Array(70000).fill(0x5a);
    for (let off = 0; off < big.length; off += 10000) await write(big.subarray(off, off + 10000));
    await new Promise(r => setTimeout(r, 20));
    const total = written.reduce((n, c) => n + c.byteLength, 0);
    check('大块直写字节数正确', total === big.length, `got=${total} expect=${big.length}`);
}

// 8. sink 会 transfer 缓冲区（workerd 字节流 write 的行为）：send 后 buffer 被 detach，
//    manualPipe 必须换新缓冲区，不能继续复用已 detach 的 bufferView。
{
    const manualPipe = makeManualPipe(4 * 1024);
    const out = [];
    let closes = 0;
    const total = 500 * 3000;
    const sink = {
        async send(chunk) {
            out.push(new Uint8Array(chunk));                            // 先同步取走数据
            structuredClone(chunk.buffer, {transfer: [chunk.buffer]});   // 再模拟 transfer/detach
            await Promise.resolve();
        }
    };
    await manualPipe(makeSource(total, 3000), sink, () => {closes++;});
    verify('transfer 型 sink 3KB×500', out, total);
    check('transfer 型 sink 不触发 close', closes === 0);
}

// 9. xHTTP 下行接线：manualPipe 直接写 responseWriter，write 的 Promise 串行化，结束后关闭响应
{
    const manualPipe = makeManualPipe(4 * 1024);
    const bridge = new TransformStream();   // 用标准 TransformStream 模拟 IdentityTransformStream
    const responseWriter = bridge.writable.getWriter();
    const collected = [];
    let maxInflight = 0, inflight = 0, closes = 0;
    const total = 400 * 3000;
    const pump = (async () => {
        const reader = bridge.readable.getReader();
        while (true) {
            const {done, value} = await reader.read();
            if (done) break;
            collected.push(value);
        }
    })();
    const sink = {
        async send(chunk) {
            inflight++, maxInflight = Math.max(maxInflight, inflight);
            const p = responseWriter.write(chunk.slice());
            await new Promise(r => setTimeout(r, 1));  // 模拟慢客户端
            await p;
            inflight--;
        }
    };
    await manualPipe(makeSource(total, 3000), sink, () => {closes++;});
    await responseWriter.close().then(() => {closes++;});
    await pump;
    verify('xHTTP 下行接线 3KB×400', collected, total);
    check('xHTTP 下行 write 串行（无并发写）', maxInflight === 1, `maxInflight=${maxInflight}`);
    check('xHTTP 下行不触发 close 且响应正常关闭', closes === 1, `closes=${closes}`);
}

// 10. manualUpload 直通：会话已建立（state.tcpWriter 直接可用），合批 + write 串行
{
    const manualUpload = makeManualUpload(() => {throw new Error('不应进入握手阶段');});
    const written = [];
    let inflight = 0, maxInflight = 0, closes = 0;
    const state = {
        tcpWriter: async chunk => {
            inflight++, maxInflight = Math.max(maxInflight, inflight);
            await new Promise(r => setTimeout(r, 1));
            written.push(chunk.slice());
            inflight--;
        }
    };
    const total = 600 * 3000;
    await manualUpload(makeSource(total, 3000).getReader({mode: 'byob'}), state, {}, {send: async () => {}}, () => {closes++;}, () => {});
    verify('上行直通 3KB×600', written, total);
    check('上行直通 write 串行（maxInflight=1）', maxInflight === 1, `maxInflight=${maxInflight}`);
    check('上行直通 有合批（不是每块一次 write）', written.length < 600, `writes=${written.length}`);
    check('上行直通 未误关连接', closes === 0);
}

// 11. manualUpload 握手：needMore 时缓冲区必须保留，补齐后与后续字节拼接不能错位
{
    const written = [];
    const state = {tcpWriter: null, tcpSocket: null, needMore: false};
    let handshakeCalls = 0, downstreamStarts = 0;
    const handleSession = async (chunk, s) => {
        handshakeCalls++;
        if (handshakeCalls === 1) return (s.needMore = true);   // 握手字节不够，继续攒
        const payload = chunk.subarray(5);                       // 前 5 字节当作已消费的头部
        s.tcpWriter = async c => {written.push(c.slice());};
        s.tcpSocket = {};
        payload.byteLength && await s.tcpWriter(payload);
        s.needMore = false;
    };
    const manualUpload = makeManualUpload(handleSession);
    const total = 200 * 1500;
    await manualUpload(makeSource(total, 1500).getReader({mode: 'byob'}), state, {}, {send: async () => {}}, () => {}, () => {downstreamStarts++;});
    const all = new Uint8Array(written.reduce((n, c) => n + c.byteLength, 0));
    let off = 0;
    for (const c of written) all.set(c, off), off += c.byteLength;
    let ok = all.byteLength === total - 5;
    for (let i = 0; ok && i < all.byteLength; i++) ok = all[i] === ((i + 5) & 0xff);
    check('上行握手 needMore 后拼接正确（无错位/丢字节）', ok, `got=${all.byteLength} expect=${total - 5}`);
    check('上行握手只建立一次会话、只启动一次下行', handshakeCalls === 2 && downstreamStarts === 1, `calls=${handshakeCalls} starts=${downstreamStarts}`);
}

// 12. 源站 writer 会 transfer 缓冲区：write 后 buffer 被 detach，上行必须换新缓冲区
{
    const manualUpload = makeManualUpload(() => {throw new Error('不应进入握手阶段');});
    const out = [];
    let closes = 0;
    const state = {
        tcpWriter: async chunk => {
            out.push(new Uint8Array(chunk));                             // 先同步取走数据
            structuredClone(chunk.buffer, {transfer: [chunk.buffer]});    // 再模拟 transfer/detach
            await Promise.resolve();
        }
    };
    const total = 400 * 3000;
    await manualUpload(makeSource(total, 3000).getReader({mode: 'byob'}), state, {}, {send: async () => {}}, () => {closes++;}, () => {});
    verify('上行 transfer 型 writer 3KB×400', out, total);
    check('上行 transfer 型 writer 不触发 close', closes === 0);
}

// 13. 背压：源站写得慢时，读循环必须被挡住，不能把整个 body 读进内存
{
    const manualUpload = makeManualUpload(() => {throw new Error('不应进入握手阶段');});
    let produced = 0, pulls = 0;
    const total = 1 << 20;
    const src = new ReadableStream({
        type: 'bytes',
        pull(controller) {
            pulls++;
            if (produced >= total) return controller.close();
            const len = Math.min(32768, total - produced);
            const buf = new Uint8Array(len);
            for (let i = 0; i < len; i++) buf[i] = (produced + i) & 0xff;
            produced += len;
            controller.enqueue(buf);
            if (produced >= total) controller.close();
        }
    });
    let release, writes = 0;
    const gate = new Promise(r => (release = r));
    const state = {tcpWriter: async () => {writes++; await gate;}};
    const running = manualUpload(src.getReader({mode: 'byob'}), state, {}, {send: async () => {}}, () => {}, () => {});
    await new Promise(r => setTimeout(r, 40));
    const pullsBlocked = pulls, writesBlocked = writes;
    release();
    await running;
    check('慢源站时读取被背压挡住（未预读整包）', pullsBlocked <= 6, `pulls=${pullsBlocked}`);
    check('慢源站时同时只有一次 write 挂起', writesBlocked === 1, `writes=${writesBlocked}`);
}

// 14. 结束时残留缓冲必须刷出（不靠定时器）
{
    const manualUpload = makeManualUpload(() => {throw new Error('不应进入握手阶段');});
    const written = [];
    const state = {tcpWriter: async c => {written.push(c.slice());}};
    await manualUpload(makeSource(1234, 1234).getReader({mode: 'byob'}), state, {}, {send: async () => {}}, () => {}, () => {});
    verify('上行结束时刷出残留（1234B）', written, 1234);
}

// 15. 队列关闭时必须 resolve 已挂起的背压 Promise，否则生产方永久挂起
{
    let closeCalls = 0, release;
    const gate = new Promise(r => (release = r));
    const queue = makeQueue(async () => {await gate; throw new Error('consume fail');}, () => {closeCalls++;}, 4, 1);
    const pending = [];
    for (let i = 0; i < 5; i++) {
        const p = queue(new Uint8Array([i]));
        p && pending.push(p);
    }
    release();
    const result = await Promise.race([
        Promise.all(pending).then(() => 'resolved'),
        new Promise(r => setTimeout(() => r('TIMEOUT'), 500))
    ]);
    check('队列关闭时挂起的生产方被唤醒', result === 'resolved', `result=${result} pending=${pending.length}`);
    check('队列关闭只触发一次 close', closeCalls === 1, `closeCalls=${closeCalls}`);
}

// 16. 静态检查：清理后不应残留已删除的状态位/脚手架，且新状态位已声明
{
    const removed = ['xwebPipeTo', 'enableChunkStats', 'recordChunkStat', 'chunkStatsHistogram', 'upBridge', 'rawTcpWriter'];
    const leaked = removed.filter(name => source.includes(name));
    check('_worker.js 无已删除标识符残留', leaked.length === 0, `leaked=${leaked.join(',')}`);

    check('xweb 状态字面量含 closed / downstreamPiped',
        /xweb: true, closed: false, downstreamPiped: false/.test(source));
    check('needMore 只在 handleSession 内清零（无重复赋值）',
        (source.match(/state\.needMore = false/g) || []).length === 1,
        `occurrences=${(source.match(/state\.needMore = false/g) || []).length}`);
}

// 17. early-data 解析：畸形头一个都不能抛（抛了就是 WS 连上 101 立刻断 + 后台 internal error）
{
    const fakeRequest = (headers, url = 'https://eswasm.qlp.de5.net/proxyip=x') => ({
        url,
        headers: {get: k => (k in headers ? headers[k] : null)}
    });
    const call = (headers, url) => {
        try {return {v: parseEarlyData(fakeRequest(headers, url))}}
        catch (e) {return {e}}
    };
    const bytes = a => a && Array.from(a).join(',');
    // 20 字节的合法 base64url 负载，用于 Referer / protocol 两条路径。
    const payload = Uint8Array.from({length: 20}, (_, i) => i);
    const payloadB64 = payload.toBase64({alphabet: 'base64url', omitPadding: true});

    let r = call({Referer: 'https://eswasm.qlp.de5.net/xyz'});
    check('带 Referer（https://host/path）不抛异常且不混入 early data',
        !r.e && r.v === null, r.e ? `${r.e.name}: ${r.e.message}` : `v=${bytes(r.v)}`);

    r = call({Referer: `eswasm.qlp.de5.net/${payloadB64}`, host: 'eswasm.qlp.de5.net'});
    check('Referer 形如 host/base64url 能解析出 early data',
        !r.e && bytes(r.v) === bytes(payload), r.e ? r.e.message : `bytes=${bytes(r.v)}`);

    r = call({'sec-websocket-protocol': '!!!not-base64!!!'});
    check('非法 Sec-WebSocket-Protocol 返回 null 且不抛',
        !r.e && r.v === null, r.e ? `${r.e.name}: ${r.e.message}` : `v=${r.v}`);

    r = call({'sec-websocket-protocol': 'AAECAw'});
    check('合法 Sec-WebSocket-Protocol 正确解码',
        !r.e && bytes(r.v) === '0,1,2,3', r.e ? r.e.message : `bytes=${bytes(r.v)}`);

    r = call({Referer: 'https://eswasm.qlp.de5.net/xyz'}, 'https://eswasm.qlp.de5.net/xyz');
    check('host 头缺失时回退 URL.host，不抛',
        !r.e, r.e ? `${r.e.name}: ${r.e.message}` : 'ok');

    r = call({Referer: 'https://eswasm.qlp.de5.net', host: 'eswasm.qlp.de5.net'});
    check('Referer 只有 host 时返回 null（不是空 Uint8Array）',
        !r.e && r.v === null, r.e ? r.e.message : `v=${r.v}`);

    r = call({});
    check('无 Referer 且无 protocol 时返回 null',
        !r.e && r.v === null, r.e ? r.e.message : `v=${r.v}`);
}

console.log(failures ? `\n${failures} 项失败` : '\n全部通过');
process.exit(failures ? 1 : 0);
