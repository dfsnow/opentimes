/* eslint-disable no-useless-assignment */
/* eslint-disable no-console */
/* eslint-disable max-params */
/* eslint-disable max-lines */
/* eslint-disable max-statements */
/* eslint-disable max-lines-per-function */
/* eslint-disable max-classes-per-file */
/* eslint-disable no-magic-numbers */
/* eslint-disable no-undef */
/* eslint-disable class-methods-use-this */
import { asyncBufferFromUrl, byteLengthFromUrl, parquetMetadataAsync, parquetRead } from "hyparquet";
import { compressors } from "hyparquet-compressors";

const BASE_URL = "https://data.opentimes.org/times/version=0.0.1/mode=car/year=2024/geography=tract",
  BIG_STATES = [],
  ZOOM_THRESHOLDS = [6, 8];

// eslint-disable-next-line one-var
const debounce = function debounce(func, wait) {
  let timeout = null;
  return function debouncedFunction(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => {
      func(...args);
    }, wait);
  };
};

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
      if (isCollapsed) {
        button.innerHTML = "&#x2b;";
      } else {
        button.innerHTML = "&#x2212;";
      }
    };
    return button;
  }

  draw(map) {
    const legendTitle = document.createElement("div");
    legendTitle.innerHTML = "<h3>Travel time<br>(driving)</h3>";
    this.scaleContainer.append(legendTitle);

    this.colors.forEach(({ color, label }) => {
      const colorBox = document.createElement("div"),
        item = document.createElement("div"),
        text = document.createElement("span");
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
    const colors = ["color_1", "color_2", "color_3", "color_4", "color_5", "color_6"],
      thresholds = this.getThresholdsForZoom(zoom);
    for (let index = 0; index < thresholds.length; index += 1) {
      if (duration < thresholds[index]) {
        return colors[index];
      }
    }

    return "none";
  }

  getLabelsForZoom(zoom) {
    if (zoom < this.zoomLower) {
      return ["< 1 hr", "1-2 hrs", "2-3 hrs", "3-4 hrs", "4-5 hrs", "5-6 hrs"];
    } else if (zoom < this.zoomUpper) {
      return ["< 30 min", "30-60 min", "1.0-1.5 hrs", "1.5-2.0 hrs", "2.5-3.0 hrs", "3.0-3.5 hrs"];
    }
    return ["< 15 min", "15-30 min", "30-45 min", "45-60 min", "60-75 min", "75-90 min"];

  }

  getThresholdsForZoom(zoom) {
    if (zoom < this.zoomLower) {
      return [3600, 7200, 10800, 14400, 21600, 28800];
    } else if (zoom < this.zoomUpper) {
      return [1800, 3600, 5400, 7200, 10800, 14400];
    }
    return [900, 1800, 2700, 3600, 5400, 7200];

  }

  updateLabels(zoom) {
    const items = this.scaleContainer.querySelectorAll("div > span"),
      labels = this.getLabelsForZoom(zoom);
    items.forEach((item, index) => {
      item.textContent = labels[index];
    });
  }
}

class Spinner {
  constructor() {
    this.spinner = this.createSpinner();
  }

  createSpinner() {
    const spinner = document.createElement("div");
    spinner.id = "map-spinner";
    return spinner;
  }

  show() {
    document.querySelector(".content").append(this.spinner);
    this.spinner.style.transform = "scaleX(0.05)";
    this.spinner.classList.remove("spinner-fade-out");
  }

  remove() {
    const contentNode = document.querySelector(".content");
    if (contentNode.contains(this.spinner)) {
      contentNode.removeChild(this.spinner);
    }
  }

  hide() {
    const contentNode = document.querySelector(".content");
    if (contentNode.contains(this.spinner)) {
      this.spinner.addEventListener("transitionend", (event) => {
        if (event.propertyName === "transform") {
          this.spinner.classList.add("spinner-fade-out");
        }
      }, { once: true });
      this.spinner.addEventListener("transitionend", (event) => {
        if (event.propertyName === "opacity") {
          contentNode.removeChild(this.spinner);
        }
      }, { once: true });
    }
  }

  updateProgress(percentage) {
    const minProgress = 5,
      progress = Math.max(percentage, minProgress);
    this.spinner.style.transform = `scaleX(${progress / 100})`;
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
    this.isProcessing = false;
  }

  init() {
    const protocol = new pmtiles.Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);

    this.map = new maplibregl.Map({
      center: [-74.0, 40.75],
      container: "map",
      doubleClickZoom: false,
      hash: true,
      maxBounds: [
        [-175.0, -9.0],
        [-20.0, 72.1],
      ],
      maxZoom: 12,
      minZoom: 2,
      pitchWithRotate: false,
      style: "https://tiles.openfreemap.org/styles/positron",
      zoom: 10,
    });

    return new Promise((resolve) => {
      this.map.on("load", () => {
        this.map.addSource("protomap", {
          promoteId: "id",
          type: "vector",
          url: "pmtiles://https://data.opentimes.org/tiles/tracts_2024.pmtiles",
        });

        this.map.addControl(new maplibregl.NavigationControl(), "bottom-right");
        this.addMapLayers(this.map);
        resolve(this.map);

        this.tractIdDisplay = this.createTractIdDisplay();
        this.addHandlers();
      });
    });
  }

  addHandlers() {
    this.map.on("mousemove", (feat) => {
      const features = this.map.queryRenderedFeatures(
        feat.point,
        { layers: ["tracts_fill"] }
      );
      // eslint-disable-next-line one-var
      const [feature] = features;
      if (feature) {
        this.map.getCanvas().style.cursor = "pointer";
        this.tractIdDisplay.style.display = "block";
        this.tractIdDisplay.textContent = `Tract ID: ${feature.properties.id}`;
        if (this.hoveredPolygonId !== null) {
          this.map.setFeatureState(
            { id: this.hoveredPolygonId, source: "protomap", sourceLayer: "tracts" },
            { hover: false }
          );
        }
        this.hoveredPolygonId = feature.properties.id;
        this.map.setFeatureState(
          { id: this.hoveredPolygonId, source: "protomap", sourceLayer: "tracts" },
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
          { id: this.hoveredPolygonId, source: "protomap", sourceLayer: "tracts" },
          { hover: false }
        );
      }
      this.hoveredPolygonId = null;
    });

    this.map.on("click", async (feat) => {
      // Do nothing if already querying
      if (this.isProcessing) {
        return;
      }
      const features = this.map.queryRenderedFeatures(
        feat.point,
        { layers: ["tracts_fill"] }
      );
      if (features.length > 0) {
        this.spinner.show();
        this.isProcessing = true;
        const [feature] = features;
        await this.processor.runQuery(
          this,
          feature.properties.state,
          feature.properties.id,
        );

        // Update the URL with ID
        window.history.replaceState({}, "", `?id=${feature.properties.id}${window.location.hash}`);
        this.isProcessing = false;
        this.spinner.hide();
      }
    });

    this.map.on("moveend", () => {
      const idParam = new URLSearchParams(window.location.search).get("id");
      if (idParam) {
        window.history.replaceState({}, "", `?id=${idParam}${window.location.hash}`);
      } else {
        window.history.replaceState({}, "", `${window.location.hash}`);
      }
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
    const { layers } = this.map.getStyle();
    // Find the index of the first symbol layer in the map style
    let firstSymbolId = null;
    for (const layer of layers) {
      if (layer.type === "symbol") {
        firstSymbolId = layer.id;
        break;
      }
    }
    this.map.addLayer({
      filter: ["==", ["geometry-type"], "Polygon"],
      id: "tracts_fill",
      paint: {
        "fill-color": [
          "case",
          ["==", ["feature-state", "tractColor"], "color_1"], "rgba(253, 231, 37, 0.4)",
          ["==", ["feature-state", "tractColor"], "color_2"], "rgba(122, 209, 81, 0.4)",
          ["==", ["feature-state", "tractColor"], "color_3"], "rgba(34, 168, 132, 0.4)",
          ["==", ["feature-state", "tractColor"], "color_4"], "rgba(42, 120, 142, 0.4)",
          ["==", ["feature-state", "tractColor"], "color_5"], "rgba(65, 68, 135, 0.4)",
          ["==", ["feature-state", "tractColor"], "color_6"], "rgba(68, 1, 84, 0.4)",
          "rgba(255, 255, 255, 0.0)"
        ],
      },
      source: "protomap",
      "source-layer": "tracts",
      type: "fill",
    }, firstSymbolId);

    this.map.addLayer({
      filter: ["==", ["geometry-type"], "Polygon"],
      id: "tracts_line",
      paint: {
        "line-color": "#333",
        "line-opacity": [
          "case",
          ["boolean", ["feature-state", "hover"], false],
          0.5,
          0.0
        ],
        "line-width": [
          "case",
          ["boolean", ["feature-state", "hover"], false],
          5,
          1
        ]
      },
      source: "protomap",
      "source-layer": "tracts",
      type: "line",
    }, firstSymbolId);
  }

  createTractIdDisplay() {
    const display = document.createElement("div");
    display.id = "map-info";
    // Initially hidden
    display.style.display = "none";
    document.body.append(display);
    return display;
  }

  updateMapFill(previousResults) {
    previousResults.forEach(row =>
      this.map.setFeatureState(
        { id: row.id, source: "protomap", sourceLayer: "tracts" },
        { tractColor: this.colorScale.getColorScale(row.duration, this.map.getZoom()) }
      )
    );
  }

  wipeMapPreviousState(previousResults) {
    previousResults.forEach(row =>
      this.map.setFeatureState(
        { id: row.id, source: "protomap", sourceLayer: "tracts" },
        { tractColor: "none" }
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
    let contentLength = null,
      metadata = null;
    if (this.byteLengthCache[url]) {
      contentLength = this.byteLengthCache[url];
    } else {
      contentLength = await byteLengthFromUrl(url);
      this.byteLengthCache[url] = contentLength;
    }

    if (this.metadataCache[url]) {
      metadata = this.metadataCache[url];
    } else {
      const buffer = await asyncBufferFromUrl({
        byteLength: Number(contentLength),
        url,
      });
      metadata = await parquetMetadataAsync(buffer);
      this.metadataCache[url] = metadata;
    }
    return metadata;
  }

  processParquetRowGroup(map, id, data, results) {
    data.forEach(row => {
      if (row[0] === id) {
        map.map.setFeatureState(
          { id: row[1], source: "protomap", sourceLayer: "tracts" },
          { tractColor: map.colorScale.getColorScale(row[2], map.map.getZoom()) }
        );
        results.push({ duration: row[2], id: row[1] });
      }
    });
  }

  async runQuery(map, state, id) {
    const queryUrl = `${this.baseUrl}/state=${state}/times-0.0.1-car-2024-tract-${state}`;

    let urlsArray = [];
    if (BIG_STATES.includes(state)) {
      urlsArray = [`${queryUrl}-0.parquet`, `${queryUrl}-1.parquet`];
    } else {
      urlsArray = [`${queryUrl}-0.parquet`];
    }

    map.wipeMapPreviousState(this.previousResults);
    this.previousResults = await this.updateMapOnQuery(map, urlsArray, id);
  }

  async readAndUpdateMap(map, id, file, metadata, rowGroup, results) {
    await parquetRead(
      {
        columns: ["origin_id", "destination_id", "duration_sec"],
        compressors,
        file,
        metadata,
        onComplete: data => this.processParquetRowGroup(map, id, data, results),
        rowEnd: rowGroup.endRow,
        rowStart: rowGroup.startRow
      }
    );
  }

  async updateMapOnQuery(map, urls, id) {
    const results = [];
    if (!this.validIdInput(id)) {
      return results;
    }

    // Initialize progress bar
    let completedGroups = 0,
      totalGroups = 0;

    // Process the metadata for each URL
    // eslint-disable-next-line one-var
    const rowGroupResults = urls.map(async (url) => {
      const contentLength = this.byteLengthCache[url],
        rowGroupMetadata = [];
      // eslint-disable-next-line one-var
      const buffer = await asyncBufferFromUrl({ byteLength: contentLength, url }),
        metadata = await this.fetchAndCacheMetadata(url);

      totalGroups += metadata.row_groups.length;

      let rowStart = 0;
      for (const rowGroup of metadata.row_groups) {
        for (const column of rowGroup.columns) {
          if (column.meta_data.path_in_schema.includes("origin_id")) {
            const
              endRow = rowStart + Number(rowGroup.num_rows) - 1,
              maxValue = column.meta_data.statistics.max_value,
              minValue = column.meta_data.statistics.min_value,
              startRow = rowStart;

            if (id >= minValue && id <= maxValue) {
              rowGroupMetadata.push({
                file: buffer,
                id,
                metadata,
                rowGroup: { endRow, startRow }
              });
            }
          }
        }
        completedGroups += 1;
        const progress = Math.ceil((completedGroups / totalGroups) * 10);
        map.spinner.updateProgress(progress);
        rowStart += Number(rowGroup.num_rows);
      }

      return rowGroupMetadata;
    });


    // Async query the rowgroups relevant to the input ID
    // eslint-disable-next-line one-var
    let rowGroupItems = await Promise.all(rowGroupResults);
    rowGroupItems = rowGroupItems.flat().filter(item => item.length !== 0);
    completedGroups = 0;

    totalGroups = rowGroupItems.length;
    if (totalGroups === 0) {
      console.warn("No data found for the given ID.");
      map.spinner.remove();
      return results;
    }

    await Promise.all(rowGroupItems.map(async (rg) => {
      await this.readAndUpdateMap(map, rg.id, rg.file, rg.metadata, rg.rowGroup, results);
      completedGroups += 1;
      const progress = Math.ceil((completedGroups / totalGroups) * 90) + 10;
      map.spinner.updateProgress(progress);
    }));
    return results;
  }

  validIdInput(id) {
    if (id && /^\d{11}$/u.test(id)) {
      return true;
    }
    console.warn("Invalid ID input. Please enter a valid 11-digit tract ID.");
    return false;
  }
}

(() => {
  const colorScale = new ColorScale(ZOOM_THRESHOLDS[0], ZOOM_THRESHOLDS[1]),
    processor = new ParquetProcessor(BASE_URL),
    spinner = new Spinner(),
    // eslint-disable-next-line sort-vars
    map = new Map(colorScale, spinner, processor);

  colorScale.draw(map.map);

  // Wait for the map to fully load before running a query
  map.map.on("load", async () => {
    // Load the previous map click if there was one
    const idParam = new URLSearchParams(window.location.search).get("id");
    if (idParam) {
      spinner.show();
      await processor.runQuery(map, idParam.substring(0, 2), idParam);
    }

    // Remove the hash if map is at starting location
    if (window.location.hash === "#10/40.75/-74") {
      console.log(window.location.hash);
      window.history.replaceState({}, "", window.location.pathname + window.location.search);
    }

    spinner.hide();
  });
})();
