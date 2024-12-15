import { asyncBufferFromUrl, byteLengthFromUrl, parquetMetadataAsync, parquetRead } from "hyparquet";
import { compressors } from "hyparquet-compressors"

const BASE_URL = "https://data.opentimes.org/times/version=0.0.1/mode=auto/year=2024/geography=tract"
const BIG_STATES = ["06", "36"];
const ZOOM_THRESHOLDS = [6, 8];

const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

class ColorScale {
  constructor(zoomLower, zoomUpper) {
    this.scaleContainer = this.createScaleContainer();
    this.toggleButton = this.createToggleButton();
    this.colors = this.getColors();
    this.zoomLower = zoomLower;
    this.zoomUpper = zoomUpper;
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

  draw(map) {
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
    map.getContainer().append(this.scaleContainer);
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

  getColorScale(duration, zoom) {
    const thresholds = this.getThresholdsForZoom(zoom);
    if (duration < thresholds[0]) return "color_1";
    if (duration < thresholds[1]) return "color_2";
    if (duration < thresholds[2]) return "color_3";
    if (duration < thresholds[3]) return "color_4";
    if (duration < thresholds[4]) return "color_5";
    if (duration < thresholds[5]) return "color_6";
    return "none";
  }

  getLabelsForZoom(zoom) {
    if (zoom < this.zoomLower) {
      return ["< 1 hr", "1-2 hrs", "2-3 hrs", "3-4 hrs", "4-5 hrs", "5-6 hrs"];
    } else if (zoom < this.zoomUpper) {
      return ["< 30 min", "30-60 min", "1.0-1.5 hrs", "1.5-2.0 hrs", "2.5-3.0 hrs", "3.0-3.5 hrs"];
    } else {
      return ["< 15 min", "15-30 min", "30-45 min", "45-60 min", "60-75 min", "75-90 min"];
    }
  }

  getThresholdsForZoom(zoom) {
    if (zoom < this.zoomLower) {
      return [3600, 7200, 10800, 14400, 21600, 28800];
    } else if (zoom < this.zoomUpper) {
      return [1800, 3600, 5400, 7200, 10800, 14400];
    } else {
      return [900, 1800, 2700, 3600, 5400, 7200];
    }
  }

  updateLabels(zoom) {
    const labels = this.getLabelsForZoom(zoom);
    const items = this.scaleContainer.querySelectorAll("div > span");
    items.forEach((item, index) => {
      item.textContent = labels[index];
    });
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

class Map {
  constructor(colorScale, spinner, processor) {
    this.init();
    this.colorScale = colorScale;
    this.spinner = spinner;
    this.processor = processor;
    this.hoveredPolygonId = null;
    this.previousZoomLevel = null;
  }

  async init() {
    this.map = new maplibregl.Map({
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
      this.map.on("load", () => {
        this.map.addSource("protomap", {
          type: "vector",
          url: "pmtiles://https://data.opentimes.org/tiles/tracts_2024.pmtiles",
          promoteId: "id",
        });

        this.map.addControl(new maplibregl.NavigationControl(), "bottom-right");
        this.addMapLayers(this.map);
        resolve(this.map);

        this.tractIdDisplay = this.createTractIdDisplay();
        this.addHandlers()
      });
    });
  }

  addHandlers() {
    this.map.on("mousemove", (e) => {
      const features = this.map.queryRenderedFeatures(
        e.point,
        { layers: ["tracts_fill"] }
      );
      const feature = features[0];
      if (feature) {
        this.map.getCanvas().style.cursor = "pointer";
        this.tractIdDisplay.style.display = "block";
        this.tractIdDisplay.textContent = `Tract ID: ${feature.properties.id}`;
        if (this.hoveredPolygonId !== null) {
          this.map.setFeatureState(
            { source: "protomap", sourceLayer: "tracts", id: this.hoveredPolygonId },
            { hover: false }
          );
        }
        this.hoveredPolygonId = feature.properties.id;
        this.map.setFeatureState(
          { source: "protomap", sourceLayer: "tracts", id: this.hoveredPolygonId },
          { hover: true }
        );
      } else {
        this.map.getCanvas().style.cursor = "";
        this.tractIdDisplay.style.display = "none";
        this.tractIdDisplay.textContent = "";
      }
    });

    // Clear hover
    this.map.on("mouseleave", () => {
      if (this.hoveredPolygonId !== null) {
        this.map.setFeatureState(
          { source: "protomap", sourceLayer: "tracts", id: this.hoveredPolygonId },
          { hover: false }
        );
      }
      this.hoveredPolygonId = null;
    });

    this.map.on("click", async (e) => {
      const features = this.map.queryRenderedFeatures(
        e.point,
        { layers: ["tracts_fill"] }
      );
      if (features.length > 0) {
        const feature = features[0];
        this.spinner.show();
        await this.processor.runQuery(
          this,
          feature.properties.state,
          feature.properties.id,
        );

        // Update the URL with ID
        window.history.replaceState({}, "", `?id=${feature.properties.id}${window.location.hash}`);
        this.spinner.hide();
      }
    });

    this.map.on("moveend", () => {
      const idParam = new URLSearchParams(window.location.search).get("id");
      window.history.replaceState({}, "", `${idParam ? `?id=${idParam}` : ""}${window.location.hash}`);
    });

    this.map.on("zoom", debounce(() => {
      const currentZoomLevel = this.map.getZoom();
      if (this.previousZoomLevel !== null) {
        const crossedThreshold = ZOOM_THRESHOLDS.some(
          (threshold) =>
            (this.previousZoomLevel < threshold && currentZoomLevel >= threshold) ||
            (this.previousZoomLevel >= threshold && currentZoomLevel < threshold)
        );

        if (crossedThreshold) {
          this.updateMapFill(this.processor.previousResults);
          this.colorScale.updateLabels(currentZoomLevel);
        }
      }
      this.previousZoomLevel = currentZoomLevel;
    }, 100));
  }

  addMapLayers() {
    const layers = this.map.getStyle().layers;
    // Find the index of the first symbol layer in the map style
    let firstSymbolId;
    for (const layer of layers) {
      if (layer.type === "symbol") {
        firstSymbolId = layer.id;
        break;
      }
    }
    this.map.addLayer({
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

    this.map.addLayer({
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

  createTractIdDisplay() {
    const display = document.createElement("div");
    display.id = "map-info";
    display.style.display = "none"; // Initially hidden
    document.body.append(display);
    return display;
  }

  updateMapFill(previousResults) {
    previousResults.forEach(row =>
      this.map.setFeatureState(
        { source: "protomap", sourceLayer: "tracts", id: row.id },
        { tract_color: this.colorScale.getColorScale(row.duration, this.map.getZoom()) }
      )
    );
  }

  wipeMapPreviousState(previousResults) {
    previousResults.forEach(row =>
      this.map.setFeatureState(
        { source: "protomap", sourceLayer: "tracts", id: row.id },
        { tract_color: "none" }
      )
    );
  }
}

class ParquetProcessor {
  constructor(url) {
    this.baseUrl = url;
    this.previousResults = [];
    this.byteLengthCache = {};
    this.metadataCache = {};
  }

  async fetchAndCacheMetadata(url) {
    let contentLength = null;
    if (!this.byteLengthCache[url]) {
      contentLength = await byteLengthFromUrl(url);
      this.byteLengthCache[url] = contentLength;
    } else {
      contentLength = this.byteLengthCache[url];
    }

    let metadata = null;
    if (!this.metadataCache[url]) {
      const buffer = await asyncBufferFromUrl({
        url,
        byteLength: parseInt(contentLength)
      });
      metadata = await parquetMetadataAsync(buffer);
      this.metadataCache[url] = metadata;
    } else {
      metadata = this.metadataCache[url];
    }
    return metadata;
  }

  async processParquetRowGroup(map, id, data, results) {
    data.forEach(row => {
      if (row[0] === id) {
        map.map.setFeatureState(
          { source: "protomap", sourceLayer: "tracts", id: row[1] },
          { tract_color: map.colorScale.getColorScale(row[2], map.map.getZoom()) }
        );
        results.push({ id: row[1], duration: row[2] });
      }
    });
  }

  async runQuery(map, state, id) {
    const queryUrl = `${this.baseUrl}/state=${state}/times-0.0.1-auto-2024-tract-${state}`;
    const urlsArray = BIG_STATES.includes(state)
      ? [`${queryUrl}-0.parquet`, `${queryUrl}-1.parquet`]
      : [`${queryUrl}-0.parquet`];

    map.wipeMapPreviousState(this.previousResults)
    this.previousResults = await this.updateMapOnQuery(map, urlsArray, id);
  }

  async updateMapOnQuery(map, urls, id) {
    const results = [];

    const dataPromises = urls.map(async (url) => {
      const metadata = await this.fetchAndCacheMetadata(url);
      const contentLength = this.byteLengthCache[url];
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
                    onComplete: data => this.processParquetRowGroup(map, id, data, results)
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
}

function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}

(async () => {
  let idParam = new URLSearchParams(window.location.search).get("id");
  const spinner = new Spinner();
  const colorScale = new ColorScale(ZOOM_THRESHOLDS[0], ZOOM_THRESHOLDS[1]);
  const processor = new ParquetProcessor(BASE_URL);

  spinner.show();

  const map = new Map(colorScale, spinner, processor);
  colorScale.draw(map.map);

  // Load the previous map click if there was one
  if (idParam) {
    await processor.runQuery(map, idParam.substring(0, 2), idParam);
  }

  // Remove the hash if map is at starting location
  if (window.location.hash === "#10/40.75/-74") {
    console.log(window.location.hash);
    window.history.replaceState({}, "", window.location.pathname + window.location.search);
  }

  spinner.hide();
})();
