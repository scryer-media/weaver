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
