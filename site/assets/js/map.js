import { asyncBufferFromUrl, byteLengthFromUrl, parquetMetadataAsync, parquetRead } from "hyparquet";
import { compressors } from "hyparquet-compressors"

const baseUrl = "https://data.opentimes.org/times/version=0.0.1/mode=auto/year=2024/geography=tract"
const bigStates = ["06", "36"];
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
    this.timeoutId = null;
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
    this.timeoutId = setTimeout(() => {
      document.querySelector(".content").append(this.overlay, this.spinner);
    }, 200);
  }

  hide() {
    if (this.timeoutId) {
      clearTimeout(this.timeoutId);
      this.timeoutId = null;
    }
    if (document.querySelector("#map-spinner")) {
      document.querySelector(".content").removeChild(this.spinner);
      document.querySelector(".content").removeChild(this.overlay);
    }
  }
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
    hash: true,
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
  const layers = map.getStyle().layers;
  // Find the index of the first symbol layer in the map style
  let firstSymbolId;
  for (const layer of layers) {
    if (layer.type === "symbol") {
      firstSymbolId = layer.id;
      break;
    }
  }
  map.addLayer({
    id: "tracts_fill",
    type: "fill",
    source: "protomap",
    "source-layer": "tracts",
    filter: ["==", ["geometry-type"], "Polygon"],
    paint: {
      "fill-color": [
        "case",
        ["==", ["feature-state", "tract_color"], "color_1"], "rgba(253, 231, 37, 0.4)",
        ["==", ["feature-state", "tract_color"], "color_2"], "rgba(122, 209, 81, 0.4)",
        ["==", ["feature-state", "tract_color"], "color_3"], "rgba(34, 168, 132, 0.4)",
        ["==", ["feature-state", "tract_color"], "color_4"], "rgba(42, 120, 142, 0.4)",
        ["==", ["feature-state", "tract_color"], "color_5"], "rgba(65, 68, 135, 0.4)",
        ["==", ["feature-state", "tract_color"], "color_6"], "rgba(68, 1, 84, 0.4)",
        "rgba(255, 255, 255, 0.0)"
      ],
    },
  }, firstSymbolId);

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
        0.0
      ],
      "line-color": "#333",
      "line-width": [
        "case",
        ["boolean", ["feature-state", "hover"], false],
        5,
        1
      ]
    },
  }, firstSymbolId);
}

function updateMapFill(map, previousResults) {
  previousResults.forEach(row =>
    map.setFeatureState(
      { source: "protomap", sourceLayer: "tracts", id: row.id },
      { tract_color: getColorScale(row.duration, map.getZoom()) }
    )
  );
}

function wipeMapPreviousState(map, previousResults) {
  previousResults.forEach(row =>
    map.setFeatureState(
      { source: "protomap", sourceLayer: "tracts", id: row.id },
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

async function processParquetData(map, id, data, results) {
  data.forEach(row => {
    if (row[0] === id) {
      map.setFeatureState(
        { source: "protomap", sourceLayer: "tracts", id: row[1] },
        { tract_color: getColorScale(row[2], map.getZoom()) }
      );
      results.push({ id: row[1], duration: row[2] });
    }
  });
}

async function fetchAndCacheMetadata(url, byteLengthCache, metadataCache) {
  let contentLength = null;
  if (!byteLengthCache[url]) {
    contentLength = await byteLengthFromUrl(url);
    byteLengthCache[url] = contentLength;
  } else {
    contentLength = byteLengthCache[url];
  }

  let metadata = null;
  if (!metadataCache[url]) {
    const buffer = await asyncBufferFromUrl({
      url,
      byteLength: parseInt(contentLength)
    });
    metadata = await parquetMetadataAsync(buffer);
    metadataCache[url] = metadata;
  } else {
    metadata = metadataCache[url];
  }

  return metadata;
}

async function updateMapFromParquet(map, urls, id, byteLengthCache, metadataCache) {
  const results = [];

  const dataPromises = urls.map(async (url) => {
    const metadata = await fetchAndCacheMetadata(url, byteLengthCache, metadataCache);
    const contentLength = byteLengthCache[url];
    const buffer = await asyncBufferFromUrl({ url, byteLength: contentLength });

    const rowGroupPromises = [];
    let rowStart = 0;

    for (const rowGroup of metadata.row_groups) {
      for (const column of rowGroup.columns) {
        if (column.meta_data.path_in_schema.includes("origin_id")) {
          const minValue = column.meta_data.statistics.min_value;
          const maxValue = column.meta_data.statistics.max_value;
          const startRow = rowStart;
          const endRow = rowStart + Number(rowGroup.num_rows) - 1;

          if (id >= minValue && id <= maxValue) {
            rowGroupPromises.push(
              parquetRead(
                {
                  file: buffer,
                  compressors: compressors,
                  metadata: metadata,
                  rowStart: startRow,
                  rowEnd: endRow,
                  columns: ["origin_id", "destination_id", "duration_sec"],
                  onComplete: data => processParquetData(map, id, data, results)
                }
              )
            );
          }
        }
      }
      rowStart += Number(rowGroup.num_rows);
    }
    await Promise.all(rowGroupPromises);
  });

  await Promise.all(dataPromises);
  return results;
}

async function runQuery(map, state, id, previousResults, byteLengthCache, metadataCache) {
  const queryUrl = `${baseUrl}/state=${state}/times-0.0.1-auto-2024-tract-${state}`;
  const urlsArray = bigStates.includes(state)
    ? [`${queryUrl}-0.parquet`, `${queryUrl}-1.parquet`]
    : [`${queryUrl}-0.parquet`];

  wipeMapPreviousState(map, previousResults)
  const results = await updateMapFromParquet(
    map, urlsArray, id, byteLengthCache, metadataCache
  );

  return results;
}

function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}

(async () => {
  let hoveredPolygonId = null;
  let previousResults = [];
  let previousZoomLevel = null;
  const byteLengthCache = {};
  const metadataCache = {};

  const spinner = new Spinner();
  spinner.show();

  const [map, tractIdDisplay] = await Promise.all([
    instantiateMap(),
    (async () => createTractIdDisplay())()
  ]);
  const colorScale = new ColorScale(map);

  // Load the previous map click if there was one
  let idParam = new URLSearchParams(window.location.search).get("id");
  if (idParam) {
    previousResults = await runQuery(
      map,
      idParam.substring(0, 2), idParam,
      previousResults,
      byteLengthCache,
      metadataCache
    );
  }

  // Remove the hash if map is at starting location
  if (window.location.hash === "#10/40.75/-74") {
    console.log(window.location.hash);
    window.history.replaceState({}, "", window.location.pathname + window.location.search);
  }

  spinner.hide();

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

  map.on("click", async (e) => {
    const features = map.queryRenderedFeatures(e.point, { layers: ["tracts_fill"] });
    if (features.length > 0) {
      spinner.show();
      const feature = features[0];
      previousResults = await runQuery(
        map,
        feature.properties.state, feature.properties.id,
        previousResults,
        byteLengthCache,
        metadataCache
      );

      // Update the URL with ID
      window.history.replaceState({}, "", `?id=${feature.properties.id}${window.location.hash}`);
      idParam = feature.properties.id;
      spinner.hide();
    }
  });

  map.on("moveend", () => {
    window.history.replaceState({}, "", `${idParam ? `?id=${idParam}` : ""}${window.location.hash}`);
  });

  map.on("zoom", debounce(() => {
    const currentZoomLevel = map.getZoom();
    if (previousZoomLevel !== null) {
      const crossedThreshold = zoomThresholds.some(
        (threshold) =>
          (previousZoomLevel < threshold && currentZoomLevel >= threshold) ||
          (previousZoomLevel >= threshold && currentZoomLevel < threshold)
      );

      if (crossedThreshold) {
        updateMapFill(map, previousResults);
        colorScale.updateLabels(currentZoomLevel);
      }
    }
    previousZoomLevel = currentZoomLevel;
  }, 100));
})();
