import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/lib/server/safe-outbound-fetch", () => ({ fetchSafeOutbound: (url: string | URL, init?: RequestInit) => fetch(url, init) }));

const mocks = vi.hoisted(() => ({
    getTask: vi.fn(),
    updateTask: vi.fn(),
    transitionTask: vi.fn(),
    schedule: vi.fn(),
    refund: vi.fn(),
}));

vi.mock("@/lib/auth/store", () => ({ refundUserPoints: mocks.refund }));
vi.mock("@/lib/server/proxy-dispatcher", () => ({ configureServerProxyDispatcher: vi.fn() }));
vi.mock("@/lib/server/generation-task-scheduler", () => ({ scheduleGenerationTask: mocks.schedule }));
vi.mock("@/lib/server/text-task-store", () => ({
    getTextTask: mocks.getTask,
    updateTextTask: mocks.updateTask,
    transitionTextTask: mocks.transitionTask,
}));

import { emptyAdvancedConfig } from "@/lib/channel-protocol-registry";
import { createProtocolFixtureServer } from "../../../scripts/protocol-fixture-server.mjs";
import { maintenanceWorkerContext } from "./maintenance-auth";
import { markTextTaskFailed, runTextTaskStep, taskHeaders } from "./text-task-runtime";
import type { TextTask, TextTaskConfig } from "./text-task-store";

describe("text task runtime recovery", () => {
    let state: TextTask;

    beforeEach(() => {
        vi.clearAllMocks();
        state = textTask(customConfig("channel-one", "https://one.example"));
        mocks.getTask.mockImplementation(async () => state);
        mocks.updateTask.mockImplementation(async (_id: string, patch: Partial<TextTask>) => {
            state = { ...state, ...patch };
            return state;
        });
        mocks.transitionTask.mockImplementation(async (_task: TextTask, allowed: string[], patch: Partial<TextTask>) => {
            if (!allowed.includes(state.status)) return null;
            state = { ...state, ...patch };
            return state;
        });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.unstubAllEnvs();
    });

    it("preserves maintenance authorization for the internal system proxy", () => {
        const token = "m".repeat(32);
        vi.stubEnv("VOZEB_PRO_MAINTENANCE_TOKEN", `${token}-maintenance`);
        vi.stubEnv("VOZEB_PRO_WORKER_TOKEN", token);

        const headers = taskHeaders({ ...openAiConfig("channel-one", "/api/ai/system/channel-one"), apiKey: "system" }, maintenanceWorkerContext("user-one"), "text-task:test:attempt:1");

        expect(headers.get("authorization")).toBe(`Bearer ${token}`);
        expect(headers.get("x-vozeb-pro-worker-user-id")).toBe("user-one");
        expect(headers.get("x-vozeb-pro-logical-model")).toBe("text-model");
        expect(headers.get("x-vozeb-pro-points-idempotency-key")).toBe("text-task:test:attempt:1");
    });

    it("completes through a live OpenAI-compatible fixture", async () => {
        const fixture = createProtocolFixtureServer();
        await new Promise<void>((resolve) => fixture.server.listen(0, "127.0.0.1", resolve));
        const address = fixture.server.address();
        if (!address || typeof address === "string") throw new Error("Protocol fixture did not bind a TCP port");
        const origin = `http://127.0.0.1:${address.port}`;
        state = textTask(openAiConfig("fixture-text", `${origin}/v1`));

        try {
            await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
            expect(state).toMatchObject({ status: "success", result: { content: "协议测试文本返回成功" } });
            expect(fixture.requests).toHaveLength(1);
            expect(fixture.requests[0]).toMatchObject({ method: "POST", path: "/v1/chat/completions" });
            expect(fixture.requests[0]?.headers.authorization).toBe("Bearer key");
        } finally {
            await new Promise<void>((resolve, reject) => fixture.server.close((error) => (error ? reject(error) : resolve())));
        }
    });

    it("streams Chat Completions through a live fixture and assembles the deltas", async () => {
        const fixture = createProtocolFixtureServer();
        await new Promise<void>((resolve) => fixture.server.listen(0, "127.0.0.1", resolve));
        const address = fixture.server.address();
        if (!address || typeof address === "string") throw new Error("Protocol fixture did not bind a TCP port");
        state = textTask(openAiConfig("fixture-text", `http://127.0.0.1:${address.port}/v1`));

        try {
            await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
            expect(state.result?.content).toBe("协议测试文本返回成功");
            expect(JSON.parse(fixture.requests[0]?.body.toString("utf8") || "{}")).toMatchObject({ model: "text-model", stream: true });
        } finally {
            await new Promise<void>((resolve, reject) => fixture.server.close((error) => (error ? reject(error) : resolve())));
        }
    });

    it("assembles Responses stream deltas and stops at response.completed", async () => {
        state = textTask(responsesConfig("channel-one", "https://one.example"));
        const fetchMock = vi.fn().mockResolvedValueOnce(
            sseResponse([
                { type: "response.created" },
                { type: "response.output_text.delta", delta: "长推理" },
                { type: "response.output_text.delta", delta: "结果" },
                { type: "response.output_text.done", text: "长推理结果" },
                { type: "response.completed" },
            ]),
        );
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        expect(state.result?.content).toBe("长推理结果");
        expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toMatchObject({ stream: true });
    });

    it("falls back to a non-stream request when the upstream rejects streaming", async () => {
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(Response.json({ error: { message: "stream is not supported" } }, { status: 400 }))
            .mockResolvedValueOnce(Response.json({ choices: [{ message: { content: "非流式结果" } }] }));
        vi.stubGlobal("fetch", fetchMock);
        state = textTask(openAiConfig("channel-one", "https://one.example"));

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        expect(fetchMock).toHaveBeenCalledTimes(2);
        expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toMatchObject({ stream: true });
        expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).not.toHaveProperty("stream");
        expect(state.result?.content).toBe("非流式结果");
    });

    it("does not request streaming when the channel disables it", async () => {
        state = textTask({ ...openAiConfig("channel-one", "https://one.example"), advancedConfig: { ...emptyAdvancedConfig(), protocol: "openai", streaming: { enabled: false } } });
        const fetchMock = vi.fn().mockResolvedValueOnce(Response.json({ choices: [{ message: { content: "关闭流式" } }] }));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).not.toHaveProperty("stream");
    });

    it("refunds and switches models when a stream reports an upstream error", async () => {
        state = textTask(openAiConfig("channel-one", "https://one.example"), [openAiConfig("channel-two", "https://two.example")]);
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(sseResponse([{ choices: [{ delta: { content: "半截" } }] }, { error: { message: "upstream overloaded" } }], { "x-vozeb-pro-points-cost": "2", "x-vozeb-pro-points-record-id": "stream-error" }))
            .mockResolvedValueOnce(sseResponse([{ choices: [{ delta: { content: "备用流式结果" } }] }, "[DONE]"]));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        expect(mocks.refund).toHaveBeenCalledWith("user-one", "text-model", 2, "text", 1, undefined, "stream-error");
        expect(state.config.channelId).toBe("channel-two");
        expect(state.attempts?.map(({ status }) => status)).toEqual(["failed", "succeeded"]);
        expect(state.result?.content).toBe("备用流式结果");
    });

    it("refunds and switches models when a stream goes idle", async () => {
        vi.useFakeTimers();
        state = textTask(openAiConfig("channel-one", "https://one.example"), [openAiConfig("channel-two", "https://two.example")]);
        const stalled = new Response(new ReadableStream({ start: (controller) => controller.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"思考中"}}]}\n\n')) }), {
            headers: { "content-type": "text/event-stream", "x-vozeb-pro-points-cost": "3", "x-vozeb-pro-points-record-id": "stream-idle" },
        });
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(stalled)
            .mockResolvedValueOnce(sseResponse([{ choices: [{ delta: { content: "备用结果" } }] }, "[DONE]"]));
        vi.stubGlobal("fetch", fetchMock);

        try {
            const step = runTextTaskStep(state, "http://internal", "");
            await vi.advanceTimersByTimeAsync(3 * 60_000 + 1);
            await expect(step).resolves.toEqual({ state: "completed" });
        } finally {
            vi.useRealTimers();
        }
        expect(mocks.refund).toHaveBeenCalledWith("user-one", "text-model", 3, "text", 1, undefined, "stream-idle");
        expect(state.attempts?.[0]).toMatchObject({ status: "failed", error: "文本模型响应超时，正在切换备用模型" });
        expect(state.result?.content).toBe("备用结果");
    });

    it("marks an interrupted stream for manual review without switching models", async () => {
        state = textTask(openAiConfig("channel-one", "https://one.example"), [openAiConfig("channel-two", "https://two.example")]);
        let sent = false;
        const broken = new Response(
            new ReadableStream({
                pull(controller) {
                    if (sent) return controller.error(new TypeError("terminated"));
                    sent = true;
                    controller.enqueue(new TextEncoder().encode('data: {"choices":[{"delta":{"content":"半截"}}]}\n\n'));
                },
            }),
            { headers: { "content-type": "text/event-stream", "x-vozeb-pro-points-cost": "4", "x-vozeb-pro-points-record-id": "stream-broken" } },
        );
        const fetchMock = vi.fn().mockResolvedValueOnce(broken);
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toMatchObject({ state: "needs_review" });
        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(state.billing).toEqual({ pointsCost: 4, pointsRecordId: "stream-broken", refunded: false });
        expect(mocks.refund).not.toHaveBeenCalled();
    });

    it("persists an asynchronous task ID and queries only one step per worker run", async () => {
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(Response.json({ task_id: "upstream-one", status: "queued" }))
            .mockResolvedValueOnce(Response.json({ status: "processing" }))
            .mockResolvedValueOnce(Response.json({ status: "completed", data: { output: "最终结果" } }));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toMatchObject({ state: "pending", upstreamTaskId: "upstream-one" });
        expect(state.upstream).toEqual({ id: "upstream-one", createPath: "/jobs" });
        expect(fetchMock).toHaveBeenCalledTimes(1);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toMatchObject({ state: "pending", status: "processing" });
        expect(fetchMock).toHaveBeenCalledTimes(2);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        expect(fetchMock).toHaveBeenCalledTimes(3);
        expect(state.status).toBe("success");
        expect(state.result?.content).toBe("最终结果");
    });

    it("does not create through another channel after a network-uncertain submission", async () => {
        state = textTask(openAiConfig("channel-one", "https://one.example"), [openAiConfig("channel-two", "https://two.example")]);
        const fetchMock = vi.fn().mockRejectedValueOnce(new Error("socket closed"));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toMatchObject({ state: "needs_review" });
        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(state.config.channelId).toBe("channel-one");
        expect(state.candidateConfigs).toHaveLength(1);
        expect(state.attempts?.map(({ status }) => status)).toEqual(["running"]);
    });

    it("automatically switches to the next text model after a timeout", async () => {
        state = textTask(openAiConfig("channel-one", "https://one.example"), [openAiConfig("channel-two", "https://two.example")]);
        const fetchMock = vi
            .fn()
            .mockRejectedValueOnce(Object.assign(new Error("request timed out"), { name: "TimeoutError" }))
            .mockResolvedValueOnce(Response.json({ choices: [{ message: { content: "备用文本结果" } }] }));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        expect(fetchMock).toHaveBeenCalledTimes(2);
        expect(String(fetchMock.mock.calls[0]?.[0])).toBe("https://one.example/v1/chat/completions");
        expect(String(fetchMock.mock.calls[1]?.[0])).toBe("https://two.example/v1/chat/completions");
        expect(state.config.channelId).toBe("channel-two");
        expect(state.attempts?.map(({ status }) => status)).toEqual(["failed", "succeeded"]);
    });

    it("switches channels after a deterministic 422 rejection", async () => {
        state = textTask(openAiConfig("channel-one", "https://one.example"), [{ ...openAiConfig("channel-two", "https://two.example"), apiFormat: "gemini" }]);
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(Response.json({ error: { message: "参数不受支持" } }, { status: 422 }))
            .mockResolvedValueOnce(Response.json({ error: { message: "参数不受支持" } }, { status: 422 }))
            .mockResolvedValueOnce(Response.json({ candidates: [{ content: { parts: [{ text: "备用渠道结果" }] } }] }));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        // 第一个渠道先流式、再非流式各被拒绝一次，然后才切换渠道。
        expect(fetchMock).toHaveBeenCalledTimes(3);
        expect(state.config.channelId).toBe("channel-two");
        expect(state.attempts?.map(({ status }) => status)).toEqual(["failed", "succeeded"]);
        expect(state.result?.content).toBe("备用渠道结果");
    });

    it("switches channels after an explicit synchronous 5xx response", async () => {
        state = textTask(openAiConfig("channel-one", "https://one.example"), [openAiConfig("channel-two", "https://two.example")]);
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(Response.json({ error: { message: "渠道暂不可用" } }, { status: 503 }))
            .mockResolvedValueOnce(Response.json({ choices: [{ message: { content: "备用渠道成功" } }] }));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        expect(fetchMock).toHaveBeenCalledTimes(2);
        expect(state.config.channelId).toBe("channel-two");
        expect(state.attempts?.map(({ status }) => status)).toEqual(["failed", "succeeded"]);
    });

    it("switches models instead of trying another protocol on the same model", async () => {
        state = textTask(responsesConfig("channel-one", "https://one.example"), [openAiConfig("channel-two", "https://two.example")]);
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(Response.json({ error: { message: "/backend-api/conversation failed: status=422, body=" } }, { status: 422 }))
            .mockResolvedValueOnce(Response.json({ error: { message: "/backend-api/conversation failed: status=422, body=" } }, { status: 422 }))
            .mockResolvedValueOnce(Response.json({ choices: [{ message: { content: "Chat 兼容返回" } }] }));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toEqual({ state: "completed" });
        expect(fetchMock).toHaveBeenCalledTimes(3);
        expect(String(fetchMock.mock.calls[0]?.[0])).toBe("https://one.example/v1/responses");
        expect(String(fetchMock.mock.calls[1]?.[0])).toBe("https://one.example/v1/responses");
        expect(String(fetchMock.mock.calls[2]?.[0])).toBe("https://two.example/v1/chat/completions");
        expect(state.config.channelId).toBe("channel-two");
        expect(state.attempts?.map(({ status }) => status)).toEqual(["failed", "succeeded"]);
        expect(state.result?.content).toBe("Chat 兼容返回");
    });

    it("marks a 2xx invalid JSON response for manual review", async () => {
        state = textTask(openAiConfig("channel-one", "https://one.example"), [openAiConfig("channel-two", "https://two.example")]);
        vi.stubGlobal(
            "fetch",
            vi.fn().mockResolvedValueOnce(
                new Response("not-json", {
                    status: 200,
                    headers: { "content-type": "application/json", "x-vozeb-pro-points-cost": "1.5", "x-vozeb-pro-points-record-id": "text-points-unknown" },
                }),
            ),
        );

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toMatchObject({ state: "needs_review" });
        expect(state.config.channelId).toBe("channel-one");
        expect(state.billing).toEqual({ pointsCost: 1.5, pointsRecordId: "text-points-unknown", refunded: false });
        expect(mocks.refund).not.toHaveBeenCalled();
    });

    it("refunds a zero-point recorded charge when the upstream task fails", async () => {
        const headers = { "x-vozeb-pro-points-cost": "0", "x-vozeb-pro-points-record-id": "record-zero" };
        const fetchMock = vi
            .fn()
            .mockResolvedValueOnce(Response.json({ task_id: "upstream-zero", status: "queued" }, { headers }))
            .mockResolvedValueOnce(Response.json({ status: "failed", error: { message: "upstream failed" } }));
        vi.stubGlobal("fetch", fetchMock);

        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toMatchObject({ state: "pending" });
        expect(state.billing).toMatchObject({ pointsCost: 0, pointsRecordId: "record-zero", refunded: false });
        await expect(runTextTaskStep(state, "http://internal", "")).resolves.toMatchObject({ state: "failed" });

        expect(mocks.refund).toHaveBeenCalledWith("user-one", "text-model", 0, "text", 1, "text-task:text-one:attempt:1:refund", "record-zero");
    });

    it("does not refund when text success wins the failure transition race", async () => {
        state = { ...textTask(openAiConfig("channel-one", "https://one.example")), status: "running", billing: { pointsCost: 2, pointsRecordId: "text-race", refunded: false } };
        mocks.transitionTask.mockImplementationOnce(async () => {
            state = { ...state, status: "success" };
            return null;
        });

        await expect(markTextTaskFailed(state, "late failure")).resolves.toEqual({ state: "completed" });
        expect(mocks.refund).not.toHaveBeenCalled();
    });

    it("commits the text error state before refunding", async () => {
        state = { ...textTask(openAiConfig("channel-one", "https://one.example")), status: "running", attemptNo: 1, billing: { pointsCost: 2, pointsRecordId: "text-failed", refunded: false } };
        mocks.refund.mockImplementationOnce(async () => {
            expect(state.status).toBe("error");
            return undefined;
        });

        await expect(markTextTaskFailed(state, "provider failed")).resolves.toEqual({ state: "failed", error: "provider failed" });
        expect(state).toMatchObject({ status: "error", billing: { refunded: true } });
        expect(mocks.refund).toHaveBeenCalledOnce();
    });
});

function sseResponse(events: Array<Record<string, unknown> | "[DONE]">, headers: Record<string, string> = {}) {
    const body = events.map((event) => `data: ${event === "[DONE]" ? event : JSON.stringify(event)}\n\n`).join("");
    return new Response(body, { headers: { "content-type": "text/event-stream", ...headers } });
}

function textTask(config: TextTaskConfig, candidateConfigs: TextTaskConfig[] = []): TextTask {
    return {
        id: "text-one",
        userId: "user-one",
        status: "pending",
        createdAt: 1,
        updatedAt: 1,
        config,
        candidateConfigs,
        messages: [{ role: "user", content: "test" }],
    };
}

function customConfig(channelId: string, baseUrl: string): TextTaskConfig {
    return {
        baseUrl,
        apiKey: "key",
        apiFormat: "openai",
        model: "text-model",
        channelId,
        advancedConfig: {
            ...emptyAdvancedConfig(),
            protocol: "custom",
            createPath: "/jobs",
            queryPath: "/jobs/{taskId}",
            requestTemplate: '{"prompt":"{{prompt}}"}',
            resultField: "data.output",
            statusField: "status",
        },
    };
}

function openAiConfig(channelId: string, baseUrl: string): TextTaskConfig {
    return { baseUrl, apiKey: "key", apiFormat: "openai", model: "text-model", channelId };
}

function responsesConfig(channelId: string, baseUrl: string): TextTaskConfig {
    return {
        ...openAiConfig(channelId, baseUrl),
        advancedConfig: {
            ...emptyAdvancedConfig(),
            protocol: "compatible",
            createPath: "/responses",
        },
    };
}
