// 读取文本模型的 SSE 流式响应，拼接 Chat Completions / Responses 的增量文本。
// 空闲超时按"多久没有收到任何字节"计算（心跳、推理增量都算活动），总时长仍由调用方的 AbortSignal 控制。

export type TextStreamKind = "chat" | "responses";

export class TextStreamIdleTimeoutError extends Error {
    constructor(readonly idleTimeoutMs: number) {
        super(`文本模型超过 ${Math.round(idleTimeoutMs / 1000)} 秒没有输出`);
        this.name = "TextStreamIdleTimeoutError";
    }
}

export class TextStreamUpstreamError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "TextStreamUpstreamError";
    }
}

export function isEventStreamResponse(response: Response) {
    return /^\s*(?:text\/event-stream|application\/x-ndjson)\b/i.test(response.headers.get("content-type") || "");
}

export async function readTextEventStream(response: Response, kind: TextStreamKind, idleTimeoutMs: number): Promise<string> {
    if (!response.body) return "";
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const accumulator = createTextStreamAccumulator(kind);
    let pending = "";
    try {
        for (;;) {
            const next = await readWithIdleTimeout(reader, idleTimeoutMs);
            if (next.done) break;
            pending += decoder.decode(next.value, { stream: true });
            const lines = pending.split(/\r?\n/);
            pending = lines.pop() || "";
            if (lines.some((line) => accumulator.append(line))) {
                await reader.cancel().catch(() => undefined);
                return accumulator.result();
            }
        }
        pending += decoder.decode();
        if (pending) accumulator.append(pending);
        return accumulator.result();
    } catch (error) {
        // 主动取消读取，让系统代理和上游及时断开连接。
        await reader.cancel().catch(() => undefined);
        throw error;
    }
}

async function readWithIdleTimeout(reader: ReadableStreamDefaultReader<Uint8Array>, idleTimeoutMs: number) {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const idle = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new TextStreamIdleTimeoutError(idleTimeoutMs)), idleTimeoutMs);
    });
    try {
        return await Promise.race([reader.read(), idle]);
    } finally {
        clearTimeout(timer);
    }
}

function createTextStreamAccumulator(kind: TextStreamKind) {
    let content = "";
    let completedText = "";
    return {
        // 返回 true 表示上游已明确结束本次流；兼容 SSE `data:` 行与 NDJSON 裸 JSON 行。
        append(line: string) {
            const trimmed = line.trim();
            const value = trimmed.startsWith("data:") ? trimmed.slice(5).trim() : trimmed;
            if (value === "[DONE]") return true;
            const payload = parseRecord(value);
            if (!payload) return false;
            const error = readStreamError(payload);
            if (error) throw new TextStreamUpstreamError(error);
            if (kind === "chat") {
                content += records(payload.choices)
                    .map((choice) => readDeltaText(record(choice.delta)?.content ?? record(choice.message)?.content))
                    .join("");
                return false;
            }
            const type = typeof payload.type === "string" ? payload.type : "";
            if (type === "response.output_text.delta" && typeof payload.delta === "string") content += payload.delta;
            if (type === "response.output_text.done" && typeof payload.text === "string") completedText += payload.text;
            return type === "response.completed";
        },
        result() {
            return content || completedText;
        },
    };
}

function readStreamError(payload: Record<string, unknown>) {
    const error = record(payload.error) || record(record(payload.response)?.error);
    if (typeof error?.message === "string" && error.message.trim()) return error.message.trim();
    if (payload.type === "error" && typeof payload.message === "string" && payload.message.trim()) return payload.message.trim();
    if (payload.type === "response.failed") return "文本模型生成失败";
    return "";
}

function readDeltaText(value: unknown) {
    if (typeof value === "string") return value;
    return records(value)
        .map((item) => (typeof item.text === "string" ? item.text : ""))
        .join("");
}

function parseRecord(value: string) {
    try {
        return record(JSON.parse(value));
    } catch {
        return undefined;
    }
}

function records(value: unknown): Record<string, unknown>[] {
    return Array.isArray(value) ? value.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object" && !Array.isArray(item)) : [];
}

function record(value: unknown): Record<string, unknown> | undefined {
    return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : undefined;
}
