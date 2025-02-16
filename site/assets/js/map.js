import { asyncBufferFromUrl, byteLengthFromUrl, parquetMetadataAsync, parquetRead } from "hyparquet";
import { Protocol } from "pmtiles";
import { compressors } from "hyparquet-compressors";
import maplibregl from "maplibre-gl";

const
  TIMES_VERSION = "0.0.1",
  TIMES_MODE = "car",
  TIMES_YEAR = "2024",
  TIMES_GEOGRAPHY = "tract";

const
  TIMES_MODES = ["car", "bicycle", "foot"],
  TIMES_YEARS = ["2020", "2021", "2022", "2023", "2024"];

const
  URL_TILES = `https://data.opentimes.org/tiles/version=${TIMES_VERSION}`,
  URL_TIMES = `https://data.opentimes.org/times/version=${TIMES_VERSION}`;

const
  MAP_CENTER = [-74.0, 40.75],
  MAP_ZOOM = 10,
  MAP_ZOOM_LIMITS = [2, 12];

const
  ZOOM_THRESHOLDS_GEOGRAPHY = {
    "block_group": [10.5, 23],
    //"county": [0, 4],
    "tract": [4, 10.5]
  },
  ZOOM_THRESHOLDS_MODE = {
    "bicycle": [8, 9.5],
    "car": [6, 8],
    "foot": [9, 10.5]
  };

// Parameters that get updated by query string or clicking the map
let modeParam = TIMES_MODE,
  yearParam = TIMES_YEAR,
  geographyParam = TIMES_GEOGRAPHY,
  stateParam = null,
  idParam = null,
  idParamBlockGroup = null,
  idParamCounty = null,
  idParamTract = null;

const debounce = function debounce(func, wait) {
  let timeout = null;
  return function debouncedFunction(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => {
      func(...args);
    }, wait);
  };
};

const getTilesUrl = function getTilesUrl({
  version = TIMES_VERSION,
  year = TIMES_YEAR,
  geography = TIMES_GEOGRAPHY
} = {}) {
  return `${URL_TILES}/year=${year}/geography=${geography}/` +
    `tiles-${version}-${year}-${geography}`;
};

const getTimesUrl = function getTimesUrl({
  version = TIMES_VERSION,
  mode = TIMES_MODE,
  year = TIMES_YEAR,
  geography = TIMES_GEOGRAPHY,
  state = null
} = {}) {
  return `${URL_TIMES}/mode=${mode}/year=${year}/geography=${geography}/` +
    `state=${state}/times-${version}-${mode}-${year}-${geography}-${state}`;
};

const getZoomGeometry = function getZoomGeometry(value) {
  for (const [key, range] of Object.entries(ZOOM_THRESHOLDS_GEOGRAPHY)) {
    if (value >= range[0] && value < range[1]) {
      return key;
    }
  }
  return null;
};

const setFeatureIds = function setFeatureIds(id, geography) {
  idParamBlockGroup = id;
  idParamTract = idParamBlockGroup.substring(0, 11);
  idParamCounty = idParamBlockGroup.substring(0, 5);
  stateParam = idParamBlockGroup.substring(0, 2);

  if (geography === "block_group") {
    idParam = idParamBlockGroup;
  } else if (geography === "tract") {
    idParam = idParamTract;
  } else if (geography === "county") {
    idParam = idParamCounty;
  }
};

const validIdInput = function validIdInput(id) {
  if (id && /^\d{5,12}$/u.test(id)) {
    return true;
  }
  console.warn("Invalid ID input. Please enter a valid Census GEOID.");
  return false;
};

const validModeInput = function validModeInput(mode) {
  if (TIMES_MODES.includes(mode)) { return true; }
  console.warn(`Invalid travel mode. Must be one of: ${TIMES_MODES.join(", ")}.`);
  return false;
};

const validYearInput = function validYearInput(year) {
  if (TIMES_YEARS.includes(year)) { return true; }
  console.warn(`Invalid data year. Must be one of: ${TIMES_YEARS.join(", ")}.`);
  return false;
};

class ColorScale {
  constructor() {
    this.scaleContainer = this.createScaleContainer();
    this.toggleButton = this.createToggleButton();
    this.modeDropdown = this.createModeDropdown();
    this.colors = this.getColors();
    this.zoomLower = null;
    this.zoomUpper = null;
  }

  createModeDropdown() {
    const container = document.createElement("div"),
      dropdown = document.createElement("select"),
      label = document.createElement("label"),
      urlParams = new URLSearchParams(window.location.search);

    container.id = "mode-dropdown";
    dropdown.id = "mode-dropdown-select";
    label.setAttribute("for", "mode-dropdown-select");
    label.textContent = "Travel mode";
    TIMES_MODES.forEach(mode => {
      const option = document.createElement("option");
      option.value = mode;
      option.textContent = mode.charAt(0).toUpperCase() + mode.slice(1);
      dropdown.appendChild(option);
    });

    modeParam = urlParams.get("mode") || TIMES_MODE;
    dropdown.value = modeParam;

    container.appendChild(label);
    container.appendChild(dropdown);

    return container;
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
    legendTitle.id = "legend-title";
    legendTitle.innerHTML = "<h2>Travel time</h2>";

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

    this.scaleContainer.append(this.modeDropdown);
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

  updateZoomThresholds(lower, upper) {
    this.zoomLower = lower;
    this.zoomUpper = upper;
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
    const protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);

    this.map = new maplibregl.Map({
      center: MAP_CENTER,
      container: "map",
      doubleClickZoom: false,
      maxBounds: [
        [-175.0, -9.0],
        [-20.0, 72.1],
      ],
      maxZoom: MAP_ZOOM_LIMITS[1],
      minZoom: MAP_ZOOM_LIMITS[0],
      pitchWithRotate: false,
      style: "https://tiles.openfreemap.org/styles/positron",
      zoom: MAP_ZOOM,
    });
    this.hash = new maplibregl.Hash();
    this.hash.addTo(this.map);
    this.hash._onHashChange();

    return new Promise((resolve) => {
      this.map.on("load", () => {
        for (const geometry in ZOOM_THRESHOLDS_GEOGRAPHY) {
          const url = getTilesUrl({ geography: geometry });
          this.map.addSource(`protomap-${geometry}`, {
            promoteId: "id",
            type: "vector",
            url: `pmtiles://${url}.pmtiles`
          });
        };

        this.map.addControl(new maplibregl.NavigationControl(), "bottom-right");
        this.addMapLayers(this.map);
        resolve(this.map);

        this.geoIdDisplay = this.createGeoIdDisplay();
        this.addHandlers();
      });
    });
  }

  addHandlers() {
    this.map.on("mousemove", (feat) => {
      const [feature] = this.map.queryRenderedFeatures(
        feat.point,
        { layers: [`geo_fill_${geographyParam}`] }
      );

      const geoName = geographyParam.split("_").
        map(word => word.charAt(0).toUpperCase() +
          word.slice(1).toLowerCase()).join(" ");

      if (feature) {
        this.map.getCanvas().style.cursor = "pointer";
        this.geoIdDisplay.style.display = "block";
        this.geoIdDisplay.textContent = `${geoName} ID: ${feature.properties.id}`;
        if (this.hoveredPolygonId !== null) {
          this.map.setFeatureState(
            {
              id: this.hoveredPolygonId,
              source: `protomap-${geographyParam}`,
              sourceLayer: "geometry"
            },
            { hover: false }
          );
        }
        this.hoveredPolygonId = feature.properties.id;
        this.map.setFeatureState(
          {
            id: this.hoveredPolygonId,
            source: `protomap-${geographyParam}`,
            sourceLayer: "geometry"
          },
          { hover: true }
        );
      } else {
        this.map.getCanvas().style.cursor = "";
        this.geoIdDisplay.style.display = "none";
        this.geoIdDisplay.textContent = "";
      }
    });

    // Clear hover
    this.map.on("mouseleave", () => {
      if (this.hoveredPolygonId !== null) {
        this.map.setFeatureState(
          {
            id: this.hoveredPolygonId,
            source: `protomap-${geographyParam}`,
            sourceLayer: "geometry"
          },
          { hover: false }
        );
      }
      this.hoveredPolygonId = null;
    });

    this.map.on("click", async (feat) => {
      // Do nothing if already querying
      if (this.isProcessing) { return; }

      const urlParams = new URLSearchParams(window.location.search);
      const [feature] = this.map.queryRenderedFeatures(
        feat.point,
        { layers: ["geo_fill_query"] }
      );
      console.log(feature);

      if (feature) {
        this.spinner.show();
        this.isProcessing = true;
        setFeatureIds(feature?.properties.id, geographyParam);

        await this.processor.runQuery(this, modeParam, yearParam, geographyParam, stateParam, idParam);

        // Update the URL with parameters
        urlParams.set("id", feature.properties.id);
        window.history.replaceState({}, "", `?${urlParams}${window.location.hash}`);
        this.isProcessing = false;
        this.spinner.hide();
      }
    });

    this.map.on("moveend", () => {

      // Only update/add the location hash after the first movement
      this.hash._updateHash();

      const urlParams = new URLSearchParams(window.location.search);
      window.history.replaceState({}, "", `${urlParams ? `?${urlParams}` : ""}${window.location.hash}`);
    });

    this.map.on("zoom", debounce(() => {
      const currentZoomLevel = this.map.getZoom();
      geographyParam = getZoomGeometry(currentZoomLevel);
      if (this.previousZoomLevel !== null) {

        const crossedGeoThreshold = ZOOM_THRESHOLDS_GEOGRAPHY[geographyParam].some(
          (threshold) =>
            (this.previousZoomLevel < threshold && currentZoomLevel >= threshold) ||
            (this.previousZoomLevel >= threshold && currentZoomLevel < threshold)
        );

        const crossedModeThreshold = ZOOM_THRESHOLDS_MODE[modeParam].some(
          (threshold) =>
            (this.previousZoomLevel < threshold && currentZoomLevel >= threshold) ||
            (this.previousZoomLevel >= threshold && currentZoomLevel < threshold)
        );

        if (crossedGeoThreshold) {
          for (const geometry in ZOOM_THRESHOLDS_GEOGRAPHY) {
            if (geometry !== geographyParam) {
              this.map.setFeatureState(
                {
                  id: this.hoveredPolygonId,
                  source: `protomap-${geographyParam}`,
                  sourceLayer: "geometry"
                },
                { hover: false }
              );
            }
          }
        };

        if (crossedModeThreshold && !this.isProcessing) {
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
    for (const geometry in ZOOM_THRESHOLDS_GEOGRAPHY) {
      this.map.addLayer({
        filter: ["==", ["geometry-type"], "Polygon"],
        id: `geo_fill_${geometry}`,
        maxzoom: ZOOM_THRESHOLDS_GEOGRAPHY[geometry][1],
        minzoom: ZOOM_THRESHOLDS_GEOGRAPHY[geometry][0],
        paint: {
          "fill-color": [
            "case",
            ["==", ["feature-state", "geoColor"], "color_1"], "rgba(253, 231, 37, 0.4)",
            ["==", ["feature-state", "geoColor"], "color_2"], "rgba(122, 209, 81, 0.4)",
            ["==", ["feature-state", "geoColor"], "color_3"], "rgba(34, 168, 132, 0.4)",
            ["==", ["feature-state", "geoColor"], "color_4"], "rgba(42, 120, 142, 0.4)",
            ["==", ["feature-state", "geoColor"], "color_5"], "rgba(65, 68, 135, 0.4)",
            ["==", ["feature-state", "geoColor"], "color_6"], "rgba(68, 1, 84, 0.4)",
            "rgba(255, 255, 255, 0.0)"
          ],
        },
        source: `protomap-${geometry}`,
        "source-layer": "geometry",
        type: "fill",
      }, firstSymbolId);

      this.map.addLayer({
        filter: ["==", ["geometry-type"], "Polygon"],
        id: `geo_line_${geometry}`,
        maxzoom: ZOOM_THRESHOLDS_GEOGRAPHY[geometry][1],
        minzoom: ZOOM_THRESHOLDS_GEOGRAPHY[geometry][0],
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
        source: `protomap-${geometry}`,
        "source-layer": "geometry",
        type: "line",
      }, firstSymbolId);
    };
  }

  createGeoIdDisplay() {
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
        {
          id: row.id,
          source: `protomap-${geographyParam}`,
          sourceLayer: "geometry"
        },
        { geoColor: this.colorScale.getColorScale(row.duration, this.map.getZoom()) }
      )
    );
  }

  wipeMapPreviousState(previousResults) {
    previousResults.forEach(row =>
      this.map.setFeatureState(
        {
          id: row.id,
          source: `protomap-${geographyParam}`,
          sourceLayer: "geometry"
        },
        { geoColor: "none" }
      )
    );
  }
}

class ParquetProcessor {
  constructor() {
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
          {
            id: row[1],
            source: `protomap-${geographyParam}`,
            sourceLayer: "geometry"
          },
          { geoColor: map.colorScale.getColorScale(row[2], map.map.getZoom()) }
        );
        results.push({ duration: row[2], id: row[1] });
      }
    });
  }

  async runQuery(map, mode, year, geography, state, id) {
    const tilesUrl = getTilesUrl({ geography }),
      queryUrl = getTimesUrl({ geography, mode, state, year });

    let filteredPreviousResults = null,
      resultIds = null,
      results = null;

    // Get the count of files given the geography, mode, and state
    const loadTileIndex = async () => await fetch(`${tilesUrl}.json`).then(response => response.json());
    const mapIndex = await loadTileIndex(),
      urlsArray = [],
      fileCount = mapIndex?.[mode]?.[state] ?? 1;
    for (let i = 0; i < fileCount; i += 1) {
      urlsArray.push(`${queryUrl}-${i}.parquet`);
    }

    results = await this.updateMapOnQuery(map, urlsArray, mode, id);
    resultIds = new Set(results.map(item => item.id));

    filteredPreviousResults = this.previousResults.filter(item => !resultIds.has(item.id));
    map.wipeMapPreviousState(filteredPreviousResults);
    this.previousResults = results;
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

  async updateMapOnQuery(map, urls, mode, id) {
    const results = [];
    if (!(validIdInput(id) && validModeInput(mode))) {
      return results;
    }

    // Initialize progress bar
    let completedGroups = 0,
      totalGroups = 0;

    // Process the metadata for each URL
    const rowGroupResults = urls.map(async (url) => {
      const contentLength = this.byteLengthCache[url],
        rowGroupMetadata = [];
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
}

(() => {
  const colorScale = new ColorScale(),
    processor = new ParquetProcessor(),
    spinner = new Spinner(),
    map = new Map(colorScale, spinner, processor);

  colorScale.draw(map.map);

  colorScale.modeDropdown.addEventListener("change", async (event) => {
    const urlParams = new URLSearchParams(window.location.search);
    let validMode = true;

    idParam = urlParams.get("id");
    modeParam = event.target.value;
    validMode = validModeInput(modeParam);

    if (validMode) {
      colorScale.updateZoomThresholds(
        ZOOM_THRESHOLDS_MODE[modeParam][0],
        ZOOM_THRESHOLDS_MODE[modeParam][1]
      );
      colorScale.updateLabels(map.map.getZoom());

      urlParams.set("mode", modeParam);
      window.history.replaceState({}, "", `?${urlParams}${window.location.hash}`);

      if (idParam) {
        setFeatureIds(idParam, geographyParam);
        spinner.show();
        await processor.runQuery(map, modeParam, yearParam, geographyParam, stateParam, idParam);
      }
    }

    spinner.hide();
  });

  // Wait for the map to fully load before running a query
  map.map.on("load", async () => {
    // Load the previous map click if there was one
    const urlParams = new URLSearchParams(window.location.search);
    let validMode = true,
      validYear = true;

    idParam = urlParams.get("id");
    modeParam = urlParams.get("mode") || TIMES_MODE;
    validMode = validModeInput(modeParam);
    yearParam = urlParams.get("year") || TIMES_YEAR;
    validYear = validYearInput(yearParam);
    geographyParam = getZoomGeometry(map.map.getZoom());

    if (validMode && validYear) {
      colorScale.updateZoomThresholds(
        ZOOM_THRESHOLDS_MODE[modeParam][0],
        ZOOM_THRESHOLDS_MODE[modeParam][1]
      );
      colorScale.updateLabels(map.map.getZoom());

      if (idParam) {
        setFeatureIds(idParam, geographyParam);
        spinner.show();
        await processor.runQuery(map, modeParam, yearParam, geographyParam, stateParam, idParam);
      }
    }

    spinner.hide();
  });
})();
