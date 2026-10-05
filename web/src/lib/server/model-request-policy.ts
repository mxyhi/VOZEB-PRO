import type { LogicalModelCapability, LogicalModelCapabilityProfile } from "@/lib/auth/store";

const MIN_REQUEST_TIMEOUT_MS = 5_000;
const MAX_REQUEST_TIMEOUT_MS = 30 * 60_000;
// 长推理文本模型（如非流式 gpt-6.1-sol）常需 3~4 分钟，默认放宽到 10 分钟；逻辑模型绑定可按 timeoutMs 覆盖。
export const TEXT_MODEL_REQUEST_TIMEOUT_MS = 10 * 60_000;
// 流式文本连续无输出的上限：gpt-6 系列近 7 天首包 p99 约 76 秒、最大约 125 秒，留出余量取 3 分钟。
export const TEXT_STREAM_IDLE_TIMEOUT_MS = 3 * 60_000;

export const DEFAULT_MODEL_REQUEST_TIMEOUT_MS: Record<LogicalModelCapability, number> = {
    text: TEXT_MODEL_REQUEST_TIMEOUT_MS,
    image: 10 * 60_000,
    video: 30 * 60_000,
    audio: 3 * 60_000,
};

type ModelRequestPolicyConfig = { capabilityProfile?: Pick<LogicalModelCapabilityProfile, "timeoutMs"> };

export function resolveModelRequestTimeoutMs(config: ModelRequestPolicyConfig | undefined, capability: LogicalModelCapability) {
    const configured = Math.floor(Number(config?.capabilityProfile?.timeoutMs));
    if (!Number.isFinite(configured) || configured <= 0) return DEFAULT_MODEL_REQUEST_TIMEOUT_MS[capability];
    return Math.max(MIN_REQUEST_TIMEOUT_MS, Math.min(MAX_REQUEST_TIMEOUT_MS, configured));
}

export function resolveModelPollingAttempts(config: ModelRequestPolicyConfig | undefined, capability: LogicalModelCapability, intervalMs: number, minimumAttempts: number) {
    return Math.max(minimumAttempts, Math.ceil(resolveModelRequestTimeoutMs(config, capability) / intervalMs));
}
