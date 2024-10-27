let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const map = new maplibregl.Map({
  style: "https://tiles.openfreemap.org/styles/positron",
  center: [-74.0, 40.75],
  zoom: 10,
  pitchWithRotate: false,
  container: "map",
})

map.on("load", async () => {
  map.addSource("protomap", {
    type: "vector",
    url: "pmtiles://https://data.opentimes.org/tiles/tracts_2024.pmtiles"
  });
  map.addControl(new maplibregl.NavigationControl());
  map.addControl(new maplibregl.ScaleControl({ unit: "metric" }));
})
