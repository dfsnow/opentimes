import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

class Spinner {
  constructor() {
    this.spinner = document.createElement("div");
    this.spinner.style.cssText = `
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      border: 16px solid #f3f3f3;
      border-top: 16px solid #3498db;
      border-radius: 50%;
      width: 6em;
      height: 6em;
      animation: spin 1s linear infinite;
      z-index: 1001;
    `;
    this.overlay = document.createElement("div");
    this.overlay.style.cssText = `
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background-color: rgba(255, 255, 255, 0.5);
      z-index: 1000;
    `;
  }

  show() {
    const contentDiv = document.querySelector(".content");
    contentDiv.style.position = "relative";
    contentDiv.appendChild(this.overlay);
    contentDiv.appendChild(this.spinner);
  }

  hide() {
    const contentDiv = document.querySelector(".content");
    contentDiv.removeChild(this.spinner);
    contentDiv.removeChild(this.overlay);
  }
}

// Spinner animation
document.head.append(Object.assign(document.createElement("style"), {
  textContent: `
    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }
  `
}));

// Initialize and return DuckDB instance
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
      "fill-opacity": 0.5,
      "fill-color": [
        "case",
        ["==", ["feature-state", "tract_color"], "color_1"], "#FDE725",
        ["==", ["feature-state", "tract_color"], "color_2"], "#B4DE2C",
        ["==", ["feature-state", "tract_color"], "color_3"], "#6DCD59",
        ["==", ["feature-state", "tract_color"], "color_4"], "#35B779",
        ["==", ["feature-state", "tract_color"], "color_5"], "#1F9E89",
        ["==", ["feature-state", "tract_color"], "color_6"], "#26828E",
        ["==", ["feature-state", "tract_color"], "color_7"], "#31688E",
        ["==", ["feature-state", "tract_color"], "color_8"], "#3E4A89",
        ["==", ["feature-state", "tract_color"], "color_9"], "#482878",
        ["==", ["feature-state", "tract_color"], "color_10"], "#440154",
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
      "line-opacity": 0.2,
      "line-color": "#333",
    },
  });
}

// Create display for current tract
function createTractIdDisplay() {
  const display = document.createElement("div");
  display.style.cssText = `
    position: absolute; bottom: 10px; left: 10px;
    background-color: rgba(255, 255, 255, 0.75);
    padding: 5px; border-radius: 5px;
    font-family: Arial, sans-serif; font-size: 1.2em;
    pointer-events: none;
  `;
  document.body.append(display);
  return display;
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

  const [DuckDB, map] = await Promise.all([instantiateDB(), instantiateMap()]);
  const db = await DuckDB.connect();
  await db.query(`
    ATTACH 'https://data.opentimes.org/databases/0.0.1.duckdb' AS opentimes (READ_ONLY);
  `);
  spinner.hide();

  const tractIdDisplay = createTractIdDisplay();

  map.on("mousemove", (e) => {
    const features = map.queryRenderedFeatures(e.point, { layers: ["tracts_fill"] });
    const feature = features[0];
    if (feature) {
      map.getCanvas().style.cursor = "pointer";
      tractIdDisplay.textContent = `Tract ID: ${feature.properties.id}`;
    } else {
      map.getCanvas().style.cursor = "";
      tractIdDisplay.textContent = "";
    }
  });

  let previousStates = [];
  map.on("click", async (e) => {
    const features = map.queryRenderedFeatures(e.point, { layers: ["tracts_fill"] });
    if (features.length > 0) {
      const feature = features[0];
      spinner.show();

      const result = await db.query(`
        SELECT destination_id, duration_sec
        FROM opentimes.internal.times_auto_2024_tract
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

