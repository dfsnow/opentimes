import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

const zoomThresholds = [6, 8];
const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

class ColorScale {
  constructor(map) {
    this.map = map;
    this.scaleContainer = this.createScaleContainer();
    this.toggleButton = this.createToggleButton();
    this.colors = this.getColors();
    this.init();
  }

  createScaleContainer() {
    const container = document.createElement("div");
    container.id = "map-color-scale";
    return container;
  }

  createToggleButton() {
    const button = document.createElement("button");
    button.id = "map-color-scale-toggle";
    button.innerHTML = "&#x2212;";
    button.onclick = () => {
      const isCollapsed = this.scaleContainer.classList.toggle("collapsed");
      button.innerHTML = isCollapsed ? "&#x2b;" : "&#x2212;";
    };
    return button;
  }

  getColors() {
    return [
      { color: "var(--map-color-1)", label: "< 15 min" },
      { color: "var(--map-color-2)", label: "15-30 min" },
      { color: "var(--map-color-3)", label: "30-45 min" },
      { color: "var(--map-color-4)", label: "45-60 min" },
      { color: "var(--map-color-5)", label: "60-75 min" },
      { color: "var(--map-color-6)", label: "75-90 min" },
    ];
  }

  init() {
    const legendTitle = document.createElement("div");
    legendTitle.innerHTML = "<h3>Travel time<br>(driving)</h3>";
    this.scaleContainer.append(legendTitle);

    this.colors.forEach(({ color, label }) => {
      const item = document.createElement("div");
      const colorBox = document.createElement("div");
      const text = document.createElement("span");
      text.textContent = label;
      colorBox.style.backgroundColor = color;
      item.append(colorBox, text);
      this.scaleContainer.append(item);
    });

    this.scaleContainer.append(this.toggleButton);
    this.map.getContainer().append(this.scaleContainer);
  }

  updateLabels(zoom) {
    const labels = this.getLabelsForZoom(zoom);
    const items = this.scaleContainer.querySelectorAll("div > span");
    items.forEach((item, index) => {
      item.textContent = labels[index];
    });
  }

  getLabelsForZoom(zoom) {
    if (zoom < zoomThresholds[0]) {
      return ["< 1 hr", "1-2 hrs", "2-3 hrs", "3-4 hrs", "4-5 hrs", "5-6 hrs"];
    } else if (zoom < zoomThresholds[1]) {
      return ["< 30 min", "30-60 min", "1.0-1.5 hrs", "1.5-2.0 hrs", "2.5-3.0 hrs", "3.0-3.5 hrs"];
    } else {
      return ["< 15 min", "15-30 min", "30-45 min", "45-60 min", "60-75 min", "75-90 min"];
    }
  }
}

class Spinner {
  constructor() {
    this.spinner = this.createSpinner();
    this.overlay = this.createOverlay();
  }

  createSpinner() {
    const spinner = document.createElement("div");
    spinner.id = "map-spinner";
    return spinner;
  }

  createOverlay() {
    const overlay = document.createElement("div");
    overlay.id = "map-overlay";
    return overlay;
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
        ["==", ["feature-state", "tract_color"], "color_2"], "rgba(122, 209, 81, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_3"], "rgba(34, 168, 132, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_4"], "rgba(42, 120, 142, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_5"], "rgba(65, 68, 135, 0.5)",
        ["==", ["feature-state", "tract_color"], "color_6"], "rgba(68, 1, 84, 0.5)",
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

function updateMapFill(map, previousStates) {
  previousStates.forEach(state =>
    map.setFeatureState(
      { source: "protomap", sourceLayer: "tracts", id: state.id },
      { tract_color: getColorScale(state.duration, map.getZoom()) }
    )
  );
}

function wipeMapPreviousState(map, previousStates) {
  previousStates.forEach(state =>
    map.setFeatureState(
      { source: "protomap", sourceLayer: "tracts", id: state.id },
      { tract_color: "none" }
    )
  );
}

function createTractIdDisplay() {
  const display = document.createElement("div");
  display.id = "map-info";
  display.style.display = "none"; // Initially hidden
  document.body.append(display);
  return display;
}

const getColorScale = (duration, zoom) => {
  const thresholds = getThresholdsForZoom(zoom);
  if (duration < thresholds[0]) return "color_1";
  if (duration < thresholds[1]) return "color_2";
  if (duration < thresholds[2]) return "color_3";
  if (duration < thresholds[3]) return "color_4";
  if (duration < thresholds[4]) return "color_5";
  if (duration < thresholds[5]) return "color_6";
  return "none";
};

function getThresholdsForZoom(zoom) {
  if (zoom < zoomThresholds[0]) {
    return [3600, 7200, 10800, 14400, 21600, 28800];
  } else if (zoom < zoomThresholds[1]) {
    return [1800, 3600, 5400, 7200, 10800, 14400];
  } else {
    return [900, 1800, 2700, 3600, 5400, 7200];
  }
}

(async () => {
  const spinner = new Spinner();
  spinner.show();

  const [DuckDB, map, tractIdDisplay] = await Promise.all([
    instantiateDB(),
    instantiateMap(),
    (async () => createTractIdDisplay())()
  ]);

  const colorScale = new ColorScale(map);
  const db = await DuckDB.connect();
  db.query("LOAD parquet");
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

      wipeMapPreviousState(map, previousStates)
      previousStates = result.toArray().map(row => {
        map.setFeatureState(
          { source: "protomap", sourceLayer: "tracts", id: row.destination_id },
          { tract_color: getColorScale(row.duration_sec, map.getZoom()) }
        );
        return { id: row.destination_id, duration: row.duration_sec };
      });
      spinner.hide();
    }
  });

  let previousZoomLevel = null;
  map.on("zoom", debounce(() => {
    const currentZoomLevel = map.getZoom();
    if (previousZoomLevel !== null) {
      const crossedThreshold = zoomThresholds.some(
        (threshold) =>
          (previousZoomLevel < threshold && currentZoomLevel >= threshold) ||
          (previousZoomLevel >= threshold && currentZoomLevel < threshold)
      );

      if (crossedThreshold) {
        updateMapFill(map, previousStates);
        colorScale.updateLabels(currentZoomLevel);
      }
    }
    previousZoomLevel = currentZoomLevel;
  }, 100));
})();

function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}
