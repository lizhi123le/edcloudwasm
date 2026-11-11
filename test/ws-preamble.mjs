// WS 握手期三项加固的行为测试：
//   1) 空帧不再被当成首包（此前会触发 handleSession 失败 → close(1011) 断连）
//   2) 首包跨帧续接：wasm 报 needMore 时累积多帧再解析
//   3) 非法首包显式 close(1008) + 独立日志（管道异常仍是 1011）
// 运行：node test/ws-preamble.mjs
//
// 抽取策略与 manual-pipe.mjs 一致：直接切 _worker.js 的真实源码执行，
// 只把 handleSession / createAsyncMicrotaskQueue 这两个外部依赖注入成桩。
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

const queueSrc = slice('const createAsyncMicrotaskQueue =', '\nconst handleSession');
// parseEarlyData + handleWebSocketConn 在源码里是连续的两段，一次切完
const wsConnSrc = slice('const parseEarlyData =', '\nconst xwebHeaders');
// handshake 累积上限是模块级常量，必须一起切进来（否则被测代码里是未定义标识符）
const maxHeadSrc = slice('const MAX_PROTOCOL_HEAD =', '\n');

const makeQueue = new Function(`${queueSrc}\nreturn createAsyncMicrotaskQueue;`)();

/** 用真实 handleWebSocketConn 源码构造处理器，注入 handleSession 桩 */
const makeWsConn = (handleSessionStub) => new Function(
    'handleSession', 'createAsyncMicrotaskQueue',
    `${maxHeadSrc}\n${wsConnSrc}\nreturn handleWebSocketConn;`
)(handleSessionStub, makeQueue);

let failures = 0;
// 用 stdout 直写而不是 console.log：部分用例会临时接管 console.log 来抓服务端日志。
const check = (name, ok, detail = '') => {
    process.stdout.write(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  ' + detail : ''}\n`);
    if (!ok) failures++;
};

const tick = (ms = 40) => new Promise((resolve) => setTimeout(resolve, ms));

const sameBytes = (a, b) => {
    if (!a || !b || a.byteLength !== b.byteLength) return false;
    for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false;
    return true;
};

const concatBytes = (...chunks) => {
    const out = new Uint8Array(chunks.reduce((n, c) => n + c.byteLength, 0));
    let off = 0;
    for (const c of chunks) { out.set(c, off); off += c.byteLength; }
    return out;
};

const makeRequest = () => ({
    url: 'https://panel.example.com/ws',
    headers: {get: () => null}
});

function makeWsStub() {
    const listeners = {};
    const closes = [];
    return {
        closes,
        addEventListener(type, fn) { (listeners[type] ||= []).push(fn); },
        close(code, reason) { closes.push({code, reason}); },
        emit(data) { for (const fn of listeners.message || []) fn({data}); }
    };
}

const toArrayBuffer = (bytes) => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);

// 1) 空帧：不进入首包解析、不关连接
{
    let sessionCalls = 0;
    const conn = makeWsConn(async () => { sessionCalls++; });
    const ws = makeWsStub();
    await conn(ws, makeRequest());
    ws.emit(new ArrayBuffer(0));
    await tick();
    check('空帧不触发首包解析', sessionCalls === 0, `sessionCalls=${sessionCalls}`);
    check('空帧不关闭连接（此前是 1011）', ws.closes.length === 0, JSON.stringify(ws.closes));
}

// 2) 首包跨帧续接：needMore → 累积 → 拼成完整首包再解析
{
    const seen = [];
    const conn = makeWsConn(async (chunk, state) => {
        seen.push(new Uint8Array(chunk).slice());
        if (seen.length === 1) {
            state.needMore = true;   // 模拟 wasm 的 RESULT[14]=1（协议头还不够）
            return;
        }
        state.tcpWriter = async () => {};   // 第二帧补齐后会话建立
    });
    const ws = makeWsStub();
    await conn(ws, makeRequest());
    const f1 = Uint8Array.from([0, 1, 2, 3, 4]);
    const f2 = Uint8Array.from([5, 6, 7, 8, 9, 10]);
    ws.emit(toArrayBuffer(f1));
    await tick();
    ws.emit(toArrayBuffer(f2));
    await tick();
    check('第一帧单独进入解析', seen.length >= 1 && sameBytes(seen[0], f1), `len=${seen[0]?.byteLength}`);
    check('needMore 后第二帧与首帧拼成完整首包',
        seen.length === 2 && sameBytes(seen[1], concatBytes(f1, f2)),
        seen.length === 2 ? `got=${Array.from(seen[1]).join(',')}` : `calls=${seen.length}`);
    check('会话建立后不再走握手累积路径', ws.closes.length === 0, JSON.stringify(ws.closes));
}

// 2b) WS state 的 allowNeedMore 确实为 true
{
    let observed = null;
    const conn = makeWsConn(async (_chunk, state) => { observed = state; });
    const ws = makeWsStub();
    await conn(ws, makeRequest());
    ws.emit(toArrayBuffer(Uint8Array.from([1])));
    await tick();
    check('state.allowNeedMore === true', observed?.allowNeedMore === true, `value=${observed?.allowNeedMore}`);
    check('state 暴露 closeInvalidFirstPacket 回调', typeof observed?.closeInvalidFirstPacket === 'function');
}

// 3) 非法首包 → close(1008) + 服务端日志带具体原因
{
    const logs = [];
    const originalLog = console.log;
    console.log = (...args) => logs.push(args.join(' '));
    try {
        const conn = makeWsConn(async (_chunk, state) => {
            state.closeInvalidFirstPacket('unrecognized protocol header');
        });
        const ws = makeWsStub();
        await conn(ws, makeRequest());
        ws.emit(toArrayBuffer(Uint8Array.from([9, 9, 9])));
        await tick();
        check('非法首包关闭码是 1008', ws.closes[0]?.code === 1008, JSON.stringify(ws.closes[0]));
        check('非法首包 close reason 不回显具体原因',
            ws.closes[0]?.reason === 'Invalid protocol or authentication failed', String(ws.closes[0]?.reason));
        check('服务端日志记录了具体原因',
            logs.some((l) => l.includes('invalid first packet: unrecognized protocol header')), logs.join(' | '));
    } finally {
        console.log = originalLog;
    }
}

// 3b) 1008 收口后：后续排队帧不再重复解析、不再重复关闭/打日志
{
    const originalLog = console.log;
    console.log = () => {};
    try {
        let calls = 0;
        const conn = makeWsConn(async (_chunk, state) => {
            calls++;
            state.closeInvalidFirstPacket('bad header');
        });
        const ws = makeWsStub();
        await conn(ws, makeRequest());
        ws.emit(toArrayBuffer(Uint8Array.from([1, 2, 3])));
        await tick(20);
        ws.emit(toArrayBuffer(Uint8Array.from([4, 5, 6])));
        await tick(20);
        check('1008 后不再重复解析首包', calls === 1, `calls=${calls}`);
        check('1008 只关闭一次', ws.closes.length === 1, JSON.stringify(ws.closes));
    } finally {
        console.log = originalLog;
    }
}

// 4) 管道异常仍然是 1011（不能和非法首包混为一谈）
{
    const conn = makeWsConn(async (_chunk, _state, _request, _writable, close) => {
        close();   // handleSession 内部走 close() 的路径（如拨号失败）
    });
    const ws = makeWsStub();
    await conn(ws, makeRequest());
    ws.emit(toArrayBuffer(Uint8Array.from([1, 2, 3])));
    await tick();
    check('管道异常仍是 1011', ws.closes[0]?.code === 1011, JSON.stringify(ws.closes[0]));
}

// 5) 累积上限：一直 needMore 且超过 2048 字节 → 按非法首包收口（不会无界增长）
{
    const originalLog = console.log;
    console.log = () => {};
    try {
        const conn = makeWsConn(async (_chunk, state) => { state.needMore = true; });
        const ws = makeWsStub();
        await conn(ws, makeRequest());
        for (let i = 0; i < 3; i++) {
            ws.emit(toArrayBuffer(new Uint8Array(800).fill(i + 1)));
            await tick(20);
        }
        check('累积超过 2048 字节仍解析不出 → 1008', ws.closes[0]?.code === 1008, JSON.stringify(ws.closes[0]));
    } finally {
        console.log = originalLog;
    }
}

// 6) 会话建立后：后续帧直通 tcpWriter，不再进入握手累积
{
    let sessionCalls = 0;
    const written = [];
    const conn = makeWsConn(async (_chunk, state) => {
        sessionCalls++;
        state.tcpWriter = async (c) => { written.push(new Uint8Array(c).slice()); };
    });
    const ws = makeWsStub();
    await conn(ws, makeRequest());
    ws.emit(toArrayBuffer(Uint8Array.from([1, 2, 3])));
    await tick();
    ws.emit(toArrayBuffer(Uint8Array.from([4, 5, 6])));
    await tick();
    check('会话建立后帧直通 tcpWriter', sameBytes(concatBytes(...written), Uint8Array.from([4, 5, 6])),
        `written=${written.map((w) => w.byteLength).join(',')}`);
    check('会话建立后不再调用 handleSession', sessionCalls === 1, `sessionCalls=${sessionCalls}`);
}

console.log(failures ? `\n${failures} 项失败` : '\n全部通过');
process.exit(failures ? 1 : 0);
