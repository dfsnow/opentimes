let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const map = new maplibregl.Map({
  style: "https://tiles.openfreemap.org/styles/positron",
  center: [-74.0, 40.75],
  zoom: 10,
  pitchWithRotate: false,
  container: "map",
  maxZoom: 12
})

map.on("load", async () => {
  map.addSource("protomap", {
    type: "vector",
    url: "pmtiles://https://data.opentimes.org/tiles/tracts_2024.pmtiles"
  });
  map.addControl(new maplibregl.NavigationControl());
  map.addControl(new maplibregl.ScaleControl({ unit: "metric" }));
  map.addLayer({
    "id": "tracts",
    "type": "line",
    "source": "protomap",
    "source-layer": "tracts",
    filter: ["==", ["geometry-type"], "Polygon"],
    "paint": {
      "line-opacity": 0.2,
      "line-color": "#000",
    },
  });
})
