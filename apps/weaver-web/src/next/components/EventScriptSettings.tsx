export const eventScriptDefaults = {
  eventScriptConcurrency: 1,
  eventScriptTimeoutSeconds: 300,
  fileDownloadedEventInterval: 0,
  scriptOutputCeilingBytes: 1048576,
  scriptOutputRunsPerJob: 32,
  scriptOutputRingBytes: 67108864,
  scriptOutputRunCapBytes: 2097152,
};

export type EventScriptOptions = typeof eventScriptDefaults;

export function eventScriptOptions(source: Partial<EventScriptOptions>): EventScriptOptions {
  return Object.fromEntries(Object.entries(eventScriptDefaults).map(([key, fallback]) =>
    [key, source[key as keyof EventScriptOptions] ?? fallback],
  )) as EventScriptOptions;
}

const fields: [keyof EventScriptOptions, string, number, number][] = [
  ["eventScriptConcurrency", "Concurrent scan, feed and scheduler scripts", 1, 8],
  ["eventScriptTimeoutSeconds", "Default event timeout (seconds)", 1, 86400],
  ["fileDownloadedEventInterval", "File event interval (seconds; -1 disables, 0 unthrottled)", -1, 86400],
  ["scriptOutputCeilingBytes", "Captured output per run (bytes)", 65536, 8388608],
  ["scriptOutputRunsPerJob", "Retained runs per job", 1, 128],
  ["scriptOutputRingBytes", "Compressed output budget (bytes)", 1048576, 1073741824],
  ["scriptOutputRunCapBytes", "Compressed output cap per run (bytes)", 65536, 8388608],
];

export function EventScriptSettings({ value, onChange }: {
  value: EventScriptOptions;
  onChange: (patch: Partial<EventScriptOptions>) => void;
}) {
  return <fieldset className="my-4 grid gap-4 sm:grid-cols-2">
    <legend className="mb-3 font-semibold">Event scripts and output retention</legend>
    {fields.map(([key, label, min, max]) => <label key={key} className="flex flex-col gap-1 text-sm">
      {label}
      <input className="border border-wv-control bg-wv-input px-3 py-2" type="number" min={min} max={max}
        value={value[key]} onChange={(event) => onChange({ [key]: Number(event.target.value) })} />
    </label>)}
    <p className="text-sm sm:col-span-2">Queue events run one at a time. Native direct-store downloads may have no archive file for scripts to inspect. Startup schedules require explicit opt-in.</p>
  </fieldset>;
}
