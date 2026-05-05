import { useEffect, useState } from "react";

function readSessionPreferences<T extends Record<string, unknown>>(key: string, defaults: T): T {
  if (typeof window === "undefined") {
    return defaults;
  }

  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) {
      return defaults;
    }

    const parsed = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed == null || Array.isArray(parsed)) {
      return defaults;
    }

    return {
      ...defaults,
      ...parsed,
    };
  } catch {
    return defaults;
  }
}

export function useTablePreferences<T extends Record<string, unknown>>(
  key: string,
  defaults: T,
) {
  const [preferences, setPreferences] = useState<T>(() => readSessionPreferences(key, defaults));

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    try {
      window.sessionStorage.setItem(key, JSON.stringify(preferences));
    } catch {
      // Ignore storage failures so the table keeps working in private mode/quota errors.
    }
  }, [key, preferences]);

  return [preferences, setPreferences] as const;
}
