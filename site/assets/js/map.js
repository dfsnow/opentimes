import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm"

let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);


async function instantiate(duckdb) {
  const CDN_BUNDLES = duckdb.getJsDelivrBundles(),
    bundle = await duckdb.selectBundle(CDN_BUNDLES), // Select a bundle based on browser checks
    worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript"
      })
    );

  const worker = new Worker(worker_url),
    logger = new duckdb.ConsoleLogger("DEBUG"),
    db = new duckdb.AsyncDuckDB(logger, worker);

  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);

  return db;
}


const map = new maplibregl.Map({
  style: "https://tiles.openfreemap.org/styles/positron",
  center: [-74.0, 40.75],
  zoom: 10,
  pitchWithRotate: false,
  doubleClickZoom: false,
  container: "map",
  maxZoom: 12
})

map.on("load", async () => {
  map.addSource("protomap", {
    type: "vector",
    url: "pmtiles://https://data.opentimes.org/tiles/tracts_2024.pmtiles"
  });
  map.addControl(new maplibregl.NavigationControl());
  map.addLayer({
    "id": "tracts_fill",
    "type": "fill",
    "source": "protomap",
    "source-layer": "tracts",
    filter: ["==", ["geometry-type"], "Polygon"],
    "paint": {
      "fill-opacity": 0.0
    },
  });
  map.addLayer({
    "id": "tracts_line",
    "type": "line",
    "source": "protomap",
    "source-layer": "tracts",
    filter: ["==", ["geometry-type"], "Polygon"],
    "paint": {
      "line-opacity": 0.2,
      "line-color": "#000",
    },
  });

  // Create a display element for the tract ID
  const tractIdDisplay = document.createElement("div");
  tractIdDisplay.style.position = "absolute";
  tractIdDisplay.style.bottom = "10px";
  tractIdDisplay.style.left = "10px";
  tractIdDisplay.style.backgroundColor = "rgba(255, 255, 255, 0.75)";
  tractIdDisplay.style.padding = "5px";
  tractIdDisplay.style.borderRadius = "5px";
  tractIdDisplay.style.fontFamily = "Arial, sans-serif";
  tractIdDisplay.style.fontSize = "1.2em";
  tractIdDisplay.style.pointerEvents = "none";
  document.body.appendChild(tractIdDisplay);

  map.on("mousemove", (e) => {
    const features = map.queryRenderedFeatures(e.point, {
      layers: ["tracts_fill"]
    });

    if (features.length > 0) {
      const feature = features[0];
      const id = feature.properties.id;
      const coordinates = feature.geometry.coordinates[0][0];
      map.getCanvas().style.cursor = "pointer";
      tractIdDisplay.textContent = `Tract ID: ${id}`;
    } else {
      map.getCanvas().style.cursor = "";
      tractIdDisplay.textContent = "";
    }
  });

  // Instantiate Duck and add a click handler
  const DuckDB = await instantiate(duckdb);
  const db = await DuckDB.connect();
  db.query(`ATTACH 'https://data.opentimes.org/databases/0.0.1.duckdb' AS opentimes (READ_ONLY);`);

  map.on("click", async (e) => {
    const features = map.queryRenderedFeatures(e.point, {
      layers: ["tracts_fill"]
    });

    if (features.length > 0) {
      const feature = features[0];
      const id = feature.properties.id;
      console.log(id);

      const result = await db.query(`
        SELECT destination_id, duration_sec
        FROM opentimes.times
        WHERE version = '0.0.1'
            AND mode = 'auto'
            AND year = '2024'
            AND geography = 'tract'
            AND centroid_type = 'weighted'
            AND origin_id = ${id}
      `);
      console.log(result);
    }
  });
})
