let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

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
  map.addControl(new maplibregl.ScaleControl({ unit: "metric", position: 'bottom-left' }));
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
  tractIdDisplay.style.bottom = "40px";
  tractIdDisplay.style.left = "10px";
  tractIdDisplay.style.backgroundColor = "rgba(255, 255, 255, 0.75)";
  tractIdDisplay.style.padding = "5px";
  tractIdDisplay.style.borderRadius = "5px";
  tractIdDisplay.style.fontFamily = "Arial, sans-serif";
  tractIdDisplay.style.fontSize = "2rem";
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
})
