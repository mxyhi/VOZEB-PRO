import { CREATIVE_UPLOAD_MAX_BYTES } from "@/lib/creative-upload";

export const SYSTEM_PROXY_JSON_BODY_MAX_BYTES = CREATIVE_UPLOAD_MAX_BYTES * 2;
// 图生图会把多张参考图打成一份 multipart。100MiB 覆盖 3 张 20MB 原图，并与常见网关 100MB 上传窗口对齐。
export const SYSTEM_PROXY_MULTIPART_BODY_MAX_BYTES = 100 * 1024 * 1024;
