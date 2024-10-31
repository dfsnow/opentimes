import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

class Spinner {
  constructor() {
    this.spinner = document.createElement("div");
    this.spinner.id = "map-spinner";
    this.overlay = document.createElement("div");
    this.overlay.id = "map-overlay";
  }

  show() {
    document.querySelector(".content").append(this.overlay, this.spinner);
  }

  hide() {
    document.querySelector(".content").removeChild(this.spinner);
    document.querySelector(".content").removeChild(this.overlay);
  }
}

async function instantiateDB() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })
  );

  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger("DEBUG"), new Worker(workerUrl));
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  return db;
}

// Initialize and configure map instance
async function instantiateMap() {
  const map = new maplibregl.Map({
    style: "https://tiles.openfreemap.org/styles/positron",
    center: [-74.0, 40.75],
    zoom: 10,
    pitchWithRotate: false,
    doubleClickZoom: false,
    container: "map",
    maxZoom: 12,
    minZoom: 2,
    maxBounds: [
      [-175.0, -9.0],
      [-20.0, 72.1],
    ],
  });

  return new Promise((resolve) => {
    map.on("load", () => {
      map.addSource("protomap", {
        type: "vector",
        url: "pmtiles://https://data.opentimes.org/tiles/tracts_2024.pmtiles",
        promoteId: "id",
      });

      map.addControl(new maplibregl.NavigationControl(), "bottom-right");
      addMapLayers(map);
      resolve(map);
    });
  });
}

function addMapLayers(map) {
  map.addLayer({
    id: "tracts_fill",
    type: "fill",
    source: "protomap",
    "source-layer": "tracts",
    filter: ["==", ["geometry-type"], "Polygon"],
    paint: {
      "fill-color": [
        "case",
        ["==", ["feature-state", "tract_color"], "color_1"], "rgba(253, 231, 37, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_2"], "rgba(180, 222, 44, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_3"], "rgba(109, 205, 89, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_4"], "rgba(53, 183, 121, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_5"], "rgba(31, 158, 137, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_6"], "rgba(38, 130, 142, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_7"], "rgba(49, 104, 142, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_8"], "rgba(62, 74, 137, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_9"], "rgba(72, 40, 120, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_10"], "rgba(68, 1, 84, 0.5)",
        "rgba(255, 255, 255, 0.0)"
      ],
    },
  });

  map.addLayer({
    id: "tracts_line",
    type: "line",
    source: "protomap",
    "source-layer": "tracts",
    filter: ["==", ["geometry-type"], "Polygon"],
    paint: {
      "line-opacity": [
        "case",
        ["boolean", ["feature-state", "hover"], false],
        0.5,
        0.2
      ],
      "line-color": "#333",
      "line-width": [
        "case",
        ["boolean", ["feature-state", "hover"], false],
        5,
        1
      ]
    },
  });
}

// Create display for current tract
function createTractIdDisplay() {
  const display = document.createElement("div");
  display.id = "map-info";
  display.style.display = "none"; // Initially hidden
  document.body.append(display);
  return display;
}

function addColorScale(map) {
  const scaleContainer = document.createElement("div");
  const toggleButton = document.createElement("button");
  scaleContainer.id = "map-color-scale";
  toggleButton.id = "map-color-scale-toggle";

  toggleButton.innerHTML = "&#x2212;"; // Unicode for minus sign
  toggleButton.onclick = () => {
    const isCollapsed = scaleContainer.classList.toggle("collapsed");
    toggleButton.innerHTML = isCollapsed ? "&#x2b;" : "&#x2212;"; // Unicode for plus and minus signs
  };

  const legendTitle = document.createElement("div");
  legendTitle.innerHTML = "<h3>Travel time<br>(driving)</h3>";
  scaleContainer.append(legendTitle);

  const colors = [
    { color: "var(--color-less-15-min)", label: "< 15 min" },
    { color: "var(--color-15-30-min)", label: "15-30 min" },
    { color: "var(--color-30-45-min)", label: "30-45 min" },
    { color: "var(--color-45-60-min)", label: "45-60 min" },
    { color: "var(--color-60-90-min)", label: "60-90 min" },
    { color: "var(--color-90-120-min)", label: "90-120 min" },
    { color: "var(--color-2-3-hrs)", label: "2-3 hrs" },
    { color: "var(--color-3-4-hrs)", label: "3-4 hrs" },
    { color: "var(--color-4-6-hrs)", label: "4-6 hrs" },
    { color: "var(--color-more-6-hrs)", label: "> 6 hrs" },
  ];

  colors.forEach(({ color, label }) => {
    const item = document.createElement("div");
    const colorBox = document.createElement("div");
    const text = document.createElement("span");
    text.textContent = label;
    colorBox.style.backgroundColor = color;
    item.append(colorBox, text);
    scaleContainer.append(item);
  });

  scaleContainer.append(toggleButton);
  map.getContainer().append(scaleContainer);
}

// Color scale based on duration
const colorScale = (duration) => {
  if (duration < 900) return "color_1";
  if (duration < 1800) return "color_2";
  if (duration < 2700) return "color_3";
  if (duration < 3600) return "color_4";
  if (duration < 5400) return "color_5";
  if (duration < 7200) return "color_6";
  if (duration < 10800) return "color_7";
  if (duration < 14400) return "color_8";
  if (duration < 21600) return "color_9";
  if (duration < 28800) return "color_10";
  return "none";
};

(async () => {
  const spinner = new Spinner();
  spinner.show();

  const [DuckDB, map, tractIdDisplay] = await Promise.all([
    instantiateDB(),
    instantiateMap(),
    (async () => {
      const display = createTractIdDisplay();
      return display;
    })()
  ]);

  const db = await DuckDB.connect();
  addColorScale(map);
  spinner.hide();

  let hoveredPolygonId = null;
  map.on("mousemove", (e) => {
    const features = map.queryRenderedFeatures(e.point, { layers: ["tracts_fill"] });
    const feature = features[0];
    if (feature) {
      map.getCanvas().style.cursor = "pointer";
      tractIdDisplay.style.display = "block";
      tractIdDisplay.textContent = `Tract ID: ${feature.properties.id}`;
      if (hoveredPolygonId !== null) {
        map.setFeatureState(
          { source: "protomap", sourceLayer: "tracts", id: hoveredPolygonId },
          { hover: false }
        );
      }
      hoveredPolygonId = feature.properties.id;
      map.setFeatureState(
        { source: "protomap", sourceLayer: "tracts", id: hoveredPolygonId },
        { hover: true }
      );
    } else {
      map.getCanvas().style.cursor = "";
      tractIdDisplay.style.display = "none";
      tractIdDisplay.textContent = "";
    }
  });

  // Clear hover
  map.on("mouseleave", () => {
    if (hoveredPolygonId !== null) {
      map.setFeatureState(
        { source: "protomap", sourceLayer: "tracts", id: hoveredPolygonId },
        { hover: false }
      );
    }
    hoveredPolygonId = null;
  });

  let previousStates = [];
  map.on("click", async (e) => {
    const features = map.queryRenderedFeatures(e.point, { layers: ["tracts_fill"] });
    if (features.length > 0) {
      spinner.show();
      const feature = features[0];
      const baseUrl = `https://data.opentimes.org/times/version=0.0.1/mode=auto/year=2024/geography=tract/state=${feature.properties.state}/times-0.0.1-auto-2024-tract-${feature.properties.state}`;
      const bigStates = ["06", "36"];
      const urlsArray = bigStates.includes(feature.properties.state)
        ? [`${baseUrl}-0.parquet`, `${baseUrl}-1.parquet`]
        : [`${baseUrl}-0.parquet`];
      const joinedUrls = urlsArray.map(url => `'${url}'`).join(',');

      const result = await db.query(`
        SELECT destination_id, duration_sec
        FROM read_parquet([${joinedUrls}])
        WHERE version = '0.0.1'
            AND mode = 'auto'
            AND year = '2024'
            AND geography = 'tract'
            AND centroid_type = 'weighted'
            AND state = '${feature.properties.state}'
            AND origin_id = '${feature.properties.id}'
      `);

      previousStates.forEach(state =>
        map.setFeatureState(
          { source: "protomap", sourceLayer: "tracts", id: state.id },
          { tract_color: "none" }
        )
      );

      previousStates = result.toArray().map(row => {
        const destinationId = row.toJSON().destination_id;
        map.setFeatureState(
          { source: "protomap", sourceLayer: "tracts", id: destinationId },
          { tract_color: colorScale(row.duration_sec) }
        );
        return { id: destinationId };
      });
      spinner.hide();
    }
  });
})();
