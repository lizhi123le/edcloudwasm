// 用真实抽取的 manualPipe 测量：上游块大小 × maxChunkLen 对读循环次数/CPU 的影响。
// 用一个最小化的 BYOB reader 模拟“上游一次交给你 N 字节”，隔离掉 ReadableStream 自身开销，
// 只测 pipe 循环的成本。注意：这不代表 Cloudflare TcpSocket 的真实分块；真机块大小分布
// 需要临时插桩（统计代码已从 _worker.js 移除），不能拿这里的数字代替。
// 运行：node test/chunk-size-bench.mjs
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, '..', '_worker.js'), 'utf8');
const start = source.indexOf('const manualPipe = async');
const end = source.indexOf('\nconst createBufferedTcpWriter', start);
const manualPipeSrc = source.slice(start, end);

const makeManualPipe = (maxChunkLen) => new Function(
    'bufferSize', 'maxChunkLen', 'flushTime', 'startThreshold', 'smallChunkThreshold',
    `${manualPipeSrc}\nreturn manualPipe;`
)(256 * 1024, maxChunkLen, 3, 50 * 1024 * 1024, 4096);

// 模拟上游：每次 read 最多给 chunkSize 字节，且不超过调用方视图长度。
function makeFakeReadable(totalBytes, chunkSize) {
    let produced = 0, reads = 0;
    return {
        get reads() { return reads; },
        getReader() {
            return {
                async read(view) {
                    if (produced >= totalBytes) return {done: true, value: undefined};
                    const len = Math.min(chunkSize, view.byteLength, totalBytes - produced);
                    const out = view.subarray(0, len);
                    out.fill(produced & 0xff);
                    produced += len, reads++;
                    return {done: false, value: out};
                }
            };
        }
    };
}

const TOTAL = 32 * 1024 * 1024;
const upstreamSizes = [1400, 4096, 16384, 65536];
const viewSizes = [4 * 1024, 16 * 1024, 64 * 1024];
const RUNS = 3;

console.log(`总字节 ${(TOTAL / 1048576).toFixed(0)}MB / 单连接 / 同步 send，取 ${RUNS} 次最小值\n`);
console.log('上游块  视图上限   读取次数     循环耗时   每 MB 读取数');

for (const up of upstreamSizes) {
    for (const view of viewSizes) {
        const manualPipe = makeManualPipe(view);
        let reads = 0, bestMs = Infinity;
        for (let r = 0; r < RUNS; r++) {
            const src = makeFakeReadable(TOTAL, up);
            let sent = 0;
            const t0 = process.hrtime.bigint();
            await manualPipe(src, {send(chunk) { sent += chunk.byteLength; }}, () => {});
            const ms = Number(process.hrtime.bigint() - t0) / 1e6;
            if (sent !== TOTAL) throw new Error(`bytes mismatch ${sent}`);
            bestMs = Math.min(bestMs, ms);
            reads = src.reads;
        }
        const perMb = reads / (TOTAL / 1048576);
        console.log(
            `${String(up).padStart(6)}B ${String(view / 1024).padStart(7)}KB ${String(reads).padStart(9)} ${bestMs.toFixed(1).padStart(8)}ms ${perMb.toFixed(0).padStart(12)}`
        );
    }
}

console.log('\n判读：');
console.log('- 「读取次数」只由 min(上游块, 视图上限) 决定，是确定值；');
console.log('- 上游块 >= 视图上限时，视图就是瓶颈，放大 maxChunkLen 可按比例减少循环次数；');
console.log('- 上游块 < 视图上限时，改 maxChunkLen 完全无效，只增加 spareBuffer 内存。');
console.log('\n是否该放大 maxChunkLen 取决于真机块大小分布（本机无法测得）：');
console.log('  若大量落在 4KB-5KB（正好等于当前视图上限）→ 值得放大；');
console.log('  若集中在 1KB-2KB（MSS 级）→ 保持不变。');
