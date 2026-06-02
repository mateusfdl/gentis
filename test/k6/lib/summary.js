export function metricValue(data, name, field) {
  const m = data.metrics[name];
  if (!m) return 'N/A';
  if (field === 'count') return m.values.count || 0;
  if (field === 'rate') return ((m.values.rate || 0) * 100).toFixed(1) + '%';
  return (m.values[field] || 0).toFixed(1);
}

export function padLine(label, value) {
  return `  ${label.padEnd(26)} ${String(value).padStart(12)}`;
}

export function summaryArtifact(data) {
  const file = __ENV.SUMMARY_FILE;
  return file ? { [file]: JSON.stringify(data, null, 2) } : {};
}
