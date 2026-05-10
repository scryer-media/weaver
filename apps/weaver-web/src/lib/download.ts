function decodeAttachmentFilename(value: string): string | null {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export function parseAttachmentFilename(contentDisposition: string | null): string | null {
  if (!contentDisposition) {
    return null;
  }

  const encodedMatch = /filename\*=UTF-8''([^;]+)/i.exec(contentDisposition);
  if (encodedMatch?.[1]) {
    return decodeAttachmentFilename(encodedMatch[1]);
  }

  const plainMatch = /filename="?([^"]+)"?/i.exec(contentDisposition);
  return plainMatch?.[1] ?? null;
}

export function saveBlobAsDownload(blob: Blob, filename: string) {
  const objectUrl = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = filename;
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => {
    window.URL.revokeObjectURL(objectUrl);
  }, 0);
}

const DOWNLOAD_FRAME_ID = "weaver-download-frame";
const DOWNLOAD_FRAME_NAME = "weaver-download-frame";

function ensureDownloadFrame(): HTMLIFrameElement {
  const existing = document.getElementById(DOWNLOAD_FRAME_ID);
  if (existing instanceof HTMLIFrameElement) {
    return existing;
  }

  const frame = document.createElement("iframe");
  frame.id = DOWNLOAD_FRAME_ID;
  frame.name = DOWNLOAD_FRAME_NAME;
  frame.hidden = true;
  frame.setAttribute("aria-hidden", "true");
  document.body.appendChild(frame);
  return frame;
}

export function submitFormAsDownload(
  action: string,
  fields: Record<string, string | null | undefined>,
) {
  const frame = ensureDownloadFrame();
  const form = document.createElement("form");
  form.method = "POST";
  form.action = action;
  form.target = frame.name;
  form.style.display = "none";

  Object.entries(fields).forEach(([name, value]) => {
    if (value == null) {
      return;
    }

    const input = document.createElement("input");
    input.type = "hidden";
    input.name = name;
    input.value = value;
    form.appendChild(input);
  });

  document.body.appendChild(form);
  form.submit();
  form.remove();
}

export async function saveResponseAsDownload(response: Response, fallbackFilename: string) {
  const blob = await response.blob();
  const filename =
    parseAttachmentFilename(response.headers.get("content-disposition")) ?? fallbackFilename;
  saveBlobAsDownload(blob, filename);
  return filename;
}

export async function readDownloadErrorMessage(
  response: Response,
  fallbackMessage: string,
): Promise<string> {
  const contentType = response.headers.get("content-type") ?? "";

  if (contentType.includes("application/json")) {
    try {
      const body = await response.json() as { error?: string | null };
      if (body.error?.trim()) {
        return body.error.trim();
      }
    } catch {
      return fallbackMessage;
    }
  }

  if (contentType.startsWith("text/")) {
    try {
      const body = await response.text();
      if (body.trim()) {
        return body.trim();
      }
    } catch {
      return fallbackMessage;
    }
  }

  return fallbackMessage;
}
