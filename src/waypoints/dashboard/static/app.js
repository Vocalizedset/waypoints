const SVG_NS = "http://www.w3.org/2000/svg";
const numberFormat = new Intl.NumberFormat("en-US");
const compactFormat = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

function svgEl(tag, attrs = {}, text = null) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(attrs)) {
    node.setAttribute(key, value);
  }
  if (text !== null) node.textContent = text;
  return node;
}

function mount(id) {
  return document.getElementById(id);
}

function empty(node, message = "No data available") {
  const box = document.createElement("div");
  box.className = "empty";
  box.textContent = message;
  node.replaceChildren(box);
}

function fmtNum(value) {
  return value === null || value === undefined ? "—" : numberFormat.format(Math.round(value));
}

function fmtCompact(value) {
  return value === null || value === undefined ? "—" : compactFormat.format(value);
}

function fmtMoney(value) {
  return value === null || value === undefined ? "—" : `$${fmtCompact(value)}`;
}

function fmtPct(value) {
  return value === null || value === undefined ? "—" : `${Number(value).toFixed(2)}%`;
}

function fmtMetric(value, digits = 2) {
  return value === null || value === undefined ? "—" : Number(value).toFixed(digits);
}

function shorten(value, max = 34) {
  const text = String(value ?? "—");
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function kpis(node, items) {
  node.replaceChildren(
    ...items.map((item) => {
      const card = document.createElement("div");
      card.className = "kpi";
      const label = document.createElement("div");
      label.className = "kpi-label";
      label.textContent = item.label;
      const value = document.createElement("div");
      value.className = "kpi-value";
      value.textContent = item.value;
      card.append(label, value);
      if (item.note) {
        const note = document.createElement("div");
        note.className = "kpi-note";
        note.textContent = item.note;
        card.append(note);
      }
      return card;
    })
  );
}

function addGrid(root, max, plot, fmt) {
  for (let i = 0; i <= 4; i += 1) {
    const value = (max * i) / 4;
    const y = plot.bottom - (value / max) * plot.height;
    root.append(svgEl("line", {
      class: i === 0 ? "axis" : "gridline",
      x1: plot.left, x2: plot.right, y1: y, y2: y,
    }));
    root.append(svgEl("text", {
      class: "tick", x: plot.left - 8, y: y + 3, "text-anchor": "end",
    }, fmt(value)));
  }
}

function lineChart(node, rows, options) {
  const data = options.maxPoints ? rows.slice(-options.maxPoints) : rows;
  if (!data.length) return empty(node);

  const W = 760;
  const H = 260;
  const plot = { left: 68, right: W - 18, top: 16, bottom: H - 42 };
  plot.height = plot.bottom - plot.top;
  const values = data.map((row) => Number(row[options.y]) || 0);
  const max = Math.max(...values, 1) * 1.12;
  const x = (index) => plot.left + (index * (plot.right - plot.left)) / Math.max(1, data.length - 1);
  const y = (value) => plot.bottom - (value / max) * plot.height;
  const root = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": options.label || "Line chart" });

  addGrid(root, max, plot, options.axisFmt || fmtCompact);

  const points = data.map((row, index) => `${x(index)},${y(Number(row[options.y]) || 0)}`).join(" ");
  root.append(svgEl("polygon", {
    class: "area",
    points: `${plot.left},${plot.bottom} ${points} ${x(data.length - 1)},${plot.bottom}`,
  }));
  root.append(svgEl("polyline", { class: options.lineClass || "line", points }));

  if (data.length <= 90) {
    data.forEach((row, index) => {
      root.append(svgEl("circle", {
        class: "point", cx: x(index), cy: y(Number(row[options.y]) || 0), r: 2.6,
      }, null));
    });
  }

  const step = Math.max(1, Math.floor(data.length / 6));
  for (let index = 0; index < data.length; index += step) {
    const label = options.xLabel ? options.xLabel(data[index]) : String(data[index][options.x]);
    root.append(svgEl("text", {
      class: "tick", x: x(index), y: H - 18, "text-anchor": "middle",
    }, shorten(label, 14)));
  }

  node.replaceChildren(root);
}

function barChart(node, rows, options) {
  if (!rows.length) return empty(node);

  const W = 760;
  const rowH = options.rowHeight || 26;
  const H = Math.max(130, rows.length * rowH + 24);
  const plot = { left: options.left || 220, right: W - 86, top: 12 };
  const width = plot.right - plot.left;
  const values = rows.map((row) => Number(row[options.value]) || 0);
  const max = Math.max(...values, 1) * 1.15;
  const root = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": options.label || "Bar chart" });

  rows.forEach((row, index) => {
    const y = plot.top + index * rowH;
    const value = Number(row[options.value]) || 0;
    const barWidth = Math.max(1, (value / max) * width);
    const label = options.labelFn ? options.labelFn(row) : String(row[options.label]);
    const formatted = options.fmt ? options.fmt(value, row) : fmtNum(value);

    root.append(svgEl("text", {
      class: "tick", x: plot.left - 10, y: y + rowH * 0.66, "text-anchor": "end",
    }, shorten(label, options.labelLength || 34)));

    const rect = svgEl("rect", {
      class: options.alt ? "bar-alt" : "bar",
      x: plot.left, y: y + 4, width: barWidth, height: rowH - 10, rx: 4,
    });
    rect.append(svgEl("title", {}, `${label}: ${formatted}`));
    root.append(rect);
    root.append(svgEl("text", {
      class: "value-label", x: plot.left + barWidth + 7, y: y + rowH * 0.66,
    }, formatted));
  });

  node.replaceChildren(root);
}

function columnChart(node, rows, options) {
  if (!rows.length) return empty(node);

  const W = 760;
  const H = 260;
  const plot = { left: 62, right: W - 18, top: 16, bottom: H - 42 };
  plot.height = plot.bottom - plot.top;
  const values = rows.map((row) => Number(row[options.value]) || 0);
  const max = Math.max(...values, 1) * 1.15;
  const step = (plot.right - plot.left) / rows.length;
  const barWidth = Math.max(2, step * 0.68);
  const root = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": options.label || "Column chart" });

  addGrid(root, max, plot, options.axisFmt || fmtCompact);

  rows.forEach((row, index) => {
    const value = Number(row[options.value]) || 0;
    const height = (value / max) * plot.height;
    const x = plot.left + index * step + (step - barWidth) / 2;
    const rect = svgEl("rect", {
      class: options.alt ? "bar-alt" : "bar",
      x, y: plot.bottom - height, width: barWidth, height, rx: 3,
    });
    const label = options.labelFn
      ? options.labelFn(row)
      : String(row[options.labelKey || options.label] ?? "");
    rect.append(svgEl("title", {}, `${label}: ${options.fmt ? options.fmt(value, row) : fmtNum(value)}`));
    root.append(rect);
    if (index % (rows.length > 30 ? 3 : 1) === 0) {
      root.append(svgEl("text", {
        class: "tick", x: x + barWidth / 2, y: H - 18, "text-anchor": "middle",
      }, label));
    }
  });

  node.replaceChildren(root);
}

function comboChart(node, rows, options) {
  if (!rows.length) return empty(node);

  const W = 760;
  const H = 260;
  const plot = { left: 68, right: W - 62, top: 16, bottom: H - 42 };
  plot.height = plot.bottom - plot.top;
  const barValues = rows.map((row) => Number(row[options.bar]) || 0);
  const lineValues = rows.map((row) => Number(row[options.line]) || 0);
  const barMax = Math.max(...barValues, 1) * 1.15;
  const lineMax = Math.max(...lineValues, 1) * 1.15;
  const step = (plot.right - plot.left) / rows.length;
  const barWidth = Math.max(3, step * 0.55);
  const x = (index) => plot.left + index * step + step / 2;
  const yBar = (value) => plot.bottom - (value / barMax) * plot.height;
  const yLine = (value) => plot.bottom - (value / lineMax) * plot.height;
  const root = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": options.label || "Combo chart" });

  addGrid(root, barMax, plot, options.barFmt || fmtCompact);
  for (let i = 0; i <= 4; i += 1) {
    const value = (lineMax * i) / 4;
    root.append(svgEl("text", {
      class: "tick", x: plot.right + 8, y: yLine(value) + 3,
    }, options.lineFmt ? options.lineFmt(value) : fmtMetric(value, 1)));
  }

  rows.forEach((row, index) => {
    const value = Number(row[options.bar]) || 0;
    const rect = svgEl("rect", {
      class: "bar", x: x(index) - barWidth / 2, y: yBar(value),
      width: barWidth, height: plot.bottom - yBar(value), rx: 3,
    });
    rect.append(svgEl("title", {}, `${row[options.x]}: ${options.barFmt ? options.barFmt(value, row) : fmtNum(value)}`));
    root.append(rect);
    root.append(svgEl("text", {
      class: "tick", x: x(index), y: H - 18, "text-anchor": "middle",
    }, String(row[options.x])));
  });

  const points = rows.map((row, index) => `${x(index)},${yLine(Number(row[options.line]) || 0)}`).join(" ");
  root.append(svgEl("polyline", { class: "line", points }));
  rows.forEach((row, index) => {
    const circle = svgEl("circle", {
      class: "point", cx: x(index), cy: yLine(Number(row[options.line]) || 0), r: 2.8,
    });
    circle.append(svgEl("title", {}, `${row[options.x]}: ${options.lineFmt ? options.lineFmt(Number(row[options.line]) || 0, row) : fmtMetric(Number(row[options.line]) || 0)}`));
    root.append(circle);
  });

  node.replaceChildren(root);
  const legend = document.createElement("div");
  legend.className = "legend";
  legend.innerHTML = `<span>Total tips</span><span class="alt">Tip percentage</span>`;
  node.append(legend);
}

function groupedBarChart(node, rows, options) {
  if (!rows.length) return empty(node);

  const W = 760;
  const H = 260;
  const plot = { left: 68, right: W - 18, top: 16, bottom: H - 42 };
  plot.height = plot.bottom - plot.top;
  const allValues = rows.flatMap((row) => options.series.map((series) => Number(series.get(row)) || 0));
  const max = Math.max(...allValues, 1) * 1.15;
  const groupWidth = (plot.right - plot.left) / rows.length;
  const barWidth = Math.max(8, (groupWidth * 0.7) / options.series.length);
  const root = svgEl("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", ariaLabel: options.label || "Grouped bar chart" });

  addGrid(root, max, plot, options.axisFmt || fmtCompact);

  rows.forEach((row, groupIndex) => {
    const center = plot.left + groupIndex * groupWidth + groupWidth / 2;
    options.series.forEach((series, seriesIndex) => {
      const value = Number(series.get(row)) || 0;
      const height = (value / max) * plot.height;
      const x = center - (options.series.length * barWidth) / 2 + seriesIndex * barWidth;
      const rect = svgEl("rect", {
        class: seriesIndex === 0 ? "bar" : "bar-alt",
        x: x + 1, y: plot.bottom - height, width: barWidth - 2, height, rx: 3,
      });
      const formatted = options.fmt ? options.fmt(value, row) : fmtNum(value);
      rect.append(svgEl("title", {}, `${row[options.category]} · ${series.name}: ${formatted}`));
      root.append(rect);
      if (height > 18) {
        root.append(svgEl("text", {
          class: "value-label", x: x + barWidth / 2, y: plot.bottom - height - 5, "text-anchor": "middle",
        }, formatted));
      }
    });
    root.append(svgEl("text", {
      class: "tick", x: center, y: H - 18, "text-anchor": "middle",
    }, String(row[options.category]).replaceAll("_", " ")));
  });

  node.replaceChildren(root);
  const legend = document.createElement("div");
  legend.className = "legend";
  options.series.forEach((series, index) => {
    const span = document.createElement("span");
    if (index > 0) span.className = "alt";
    span.textContent = series.name;
    legend.append(span);
  });
  node.append(legend);
}

function table(node, rows, columns, options = {}) {
  if (!rows.length) return empty(node);

  const tableEl = document.createElement("table");
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column.label;
    headRow.append(th);
  });
  thead.append(headRow);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    if (options.bestKey && row[options.bestKey]) tr.className = "best";
    columns.forEach((column) => {
      const td = document.createElement("td");
      if (column.badge) {
        const badge = document.createElement("span");
        const ok = Boolean(row[column.key]);
        badge.className = `badge ${ok ? "ok" : "bad"}`;
        badge.textContent = ok ? "MATCH" : "MISMATCH";
        td.append(badge);
      } else {
        const value = column.fmt ? column.fmt(row[column.key], row) : row[column.key];
        td.textContent = value === null || value === undefined ? "—" : String(value);
      }
      tr.append(td);
    });
    tbody.append(tr);
  });

  tableEl.append(thead, tbody);
  node.replaceChildren(tableEl);
}

function pivotExperiment(rows) {
  const scenarios = [...new Set(rows.map((row) => row.scenario))];
  return scenarios.map((scenario) => ({
    scenario,
    batch: rows.find((row) => row.scenario === scenario && row.approach === "batch") || {},
    incremental: rows.find((row) => row.scenario === scenario && row.approach === "incremental") || {},
  }));
}

function renderOverview(data) {
  const summary = data.revenue.summary || {};
  const tipRate = summary.total_fare_amount
    ? (100 * summary.total_tips) / summary.total_fare_amount
    : null;
  kpis(mount("overview-kpis"), [
    { label: "Total revenue", value: fmtMoney(summary.total_revenue), note: "from total_amount" },
    { label: "Total tips", value: fmtMoney(summary.total_tips), note: "all recorded months" },
    { label: "Aggregate tip rate", value: fmtPct(tipRate), note: "tips ÷ positive fares" },
    { label: "Trips analysed", value: fmtNum(summary.trip_count), note: "cleaned feature rows" },
  ]);
}

function renderRevenue(data) {
  lineChart(mount("revenue-trend"), data.revenue.trend, {
    x: "month", y: "total_revenue", label: "Monthly revenue trend", fmt: fmtMoney,
  });
  barChart(mount("revenue-borough"), data.revenue.by_borough, {
    label: "pickup_borough", value: "total_revenue", fmt: fmtMoney, labelLength: 20,
  });
  barChart(mount("payment-method"), data.revenue.by_payment, {
    label: "payment_method", value: "total_revenue", fmt: fmtMoney, labelLength: 24,
  });
  comboChart(mount("tips"), data.revenue.tips, {
    x: "month", bar: "total_tips", line: "tip_percentage",
    barFmt: fmtMoney, lineFmt: (value) => fmtMetric(value, 2) + "%",
  });
}

function renderDemand(data) {
  lineChart(mount("daily-demand"), data.demand.daily, {
    x: "pickup_date", y: "trip_count", label: "Daily demand",
  });
  lineChart(mount("hourly-demand"), data.demand.hourly, {
    x: "pickup_hour_start", y: "trip_count", label: "Hourly demand", maxPoints: 30,
    xLabel: (row) => String(row.pickup_hour_start).slice(5, 16).replace("T", " "),
  });
  const hours = [...data.demand.peak_hours].sort((a, b) => a.hour_label.localeCompare(b.hour_label));
  columnChart(mount("peak-hours"), hours, {
    label: "Peak pickup hours", labelKey: "hour_label", value: "trip_count",
    labelFn: (row) => row.hour_label.slice(0, 2),
  });
  barChart(mount("busiest-zones"), data.demand.busiest_zones.map((row) => ({
    label: `${row.pickup_borough}: ${row.pickup_zone}`,
    trips: row.trip_count,
  })), { label: "label", value: "trips", labelLength: 32 });

  barChart(mount("popular-routes"), data.demand.popular_routes, {
    label: "route", value: "trip_count", labelLength: 44, left: 280,
  });

  barChart(mount("weekday-weekend"), data.demand.weekday_weekend.map((row) => ({
    label: `${row.day_type} (${fmtPct(row.demand_share_pct)})`,
    trips: row.trip_count,
  })), { label: "label", value: "trips", labelLength: 24 });
}

function renderOperations(data) {
  const ops = data.operations;
  kpis(mount("operations-kpis"), [
    {
      label: "Avg trip duration",
      value: `${fmtMetric(ops.avg_duration.avg_trip_duration_minutes)} min`,
      note: `${fmtNum(ops.avg_duration.valid_trip_count)} valid trips`,
    },
    {
      label: "Avg trip distance",
      value: `${fmtMetric(ops.avg_distance.avg_trip_distance_miles)} mi`,
      note: `${fmtNum(ops.avg_distance.valid_trip_count)} valid trips`,
    },
    {
      label: "Avg speed",
      value: `${fmtMetric(ops.avg_speed.avg_speed_mph)} mph`,
      note: `${fmtNum(ops.avg_speed.valid_trip_count)} valid trips`,
    },
    {
      label: "Fare per mile",
      value: `$${fmtMetric(ops.fare_per_mile.avg_fare_per_mile)}`,
      note: `${fmtNum(ops.fare_per_mile.valid_trip_count)} valid trips`,
    },
  ]);

  barChart(mount("passenger-distribution"), ops.passengers.map((row) => ({
    label: `${row.passenger_count_bucket} passengers (${fmtPct(row.trip_share_pct)})`,
    trips: row.trip_count,
  })), { label: "label", value: "trips", labelLength: 32 });
}

function renderML(data) {
  table(mount("model-table"), data.ml, [
    { key: "model_name", label: "Model" },
    { key: "rmse", label: "RMSE", fmt: (value) => fmtMetric(value, 3) },
    { key: "mae", label: "MAE", fmt: (value) => fmtMetric(value, 3) },
    { key: "r2", label: "R²", fmt: (value) => fmtMetric(value, 3) },
    { key: "is_best", label: "Selection", badge: true },
  ], { bestKey: "is_best" });
}

function renderExperiment(data) {
  const pivoted = pivotExperiment(data.experiment);
  const series = [
    { name: "Batch", get: (row) => row.batch.execution_seconds },
    { name: "Incremental", get: (row) => row.incremental.execution_seconds },
  ];
  groupedBarChart(mount("exp-time"), pivoted, {
    category: "scenario", series, label: "Execution time",
    fmt: (value) => `${fmtMetric(value, 1)}s`,
  });
  groupedBarChart(mount("exp-records"), pivoted, {
    category: "scenario",
    series: [
      { name: "Batch", get: (row) => row.batch.source_records_processed },
      { name: "Incremental", get: (row) => row.incremental.source_records_processed },
    ],
    label: "Records processed", fmt: fmtCompact,
  });
  groupedBarChart(mount("exp-throughput"), pivoted, {
    category: "scenario",
    series: [
      { name: "Batch", get: (row) => row.batch.throughput_records_per_second },
      { name: "Incremental", get: (row) => row.incremental.throughput_records_per_second },
    ],
    label: "Throughput", fmt: (value) => `${fmtCompact(value)}/s`,
  });

  table(mount("exp-correctness"), data.experiment, [
    { key: "scenario", label: "Scenario", fmt: (value) => String(value).replaceAll("_", " ") },
    { key: "approach", label: "Approach", fmt: (value) => String(value).toUpperCase() },
    { key: "outputs_compared", label: "Outputs compared" },
    { key: "correctness_match", label: "Result", badge: true },
    { key: "mismatched_outputs", label: "Mismatches", fmt: (value) => value || "none" },
  ]);
}

function renderSources(data) {
  const node = mount("sources");
  node.replaceChildren(
    ...data.sources.map((source) => {
      const span = document.createElement("span");
      span.textContent = source.file;
      span.title = `Modified ${source.modified_at}`;
      return span;
    })
  );
}

function renderAll(data) {
  renderOverview(data);
  renderRevenue(data);
  renderDemand(data);
  renderOperations(data);
  renderML(data);
  renderExperiment(data);
  renderSources(data);
}

fetch("/api/dashboard")
  .then((response) => {
    if (!response.ok) return response.json().then((body) => { throw new Error(body.detail || response.statusText); });
    return response.json();
  })
  .then(renderAll)
  .catch((error) => {
    const status = mount("status");
    status.hidden = false;
    status.textContent = `Unable to load dashboard data: ${error.message}. Run the pipeline and train steps first.`;
  });
