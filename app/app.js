module.exports = function(RED) {
  "use strict";

  function convertData(config) {
      RED.nodes.createNode(this, config);
      var node = this;

      node.on('input', function(msg) {
        // get data type
        const dataType = msg.req.headers["x-data-type"];
        // get data
        var data = {};
        data = msg.req.body;
        // result
        var formatedData = {};

        switch(dataType) {
          case "OPEN_DATA_WIFI": 
            formatedData = convertOpenDataWifi(data);
            break;

          case "OPEN_DATA_BUS_POSITION_REAL_TIME": 
            formatedData = convertOpenDataBusPositionRealTime(data);
            break;
        }

        msg.payload = formatedData;
        node.send(msg);
      });
  }

  /**
   * [convertOpenDataWifi description]
   * @param  {[type]} data [description]
   * @return {[type]}      [description]
   */
  function convertOpenDataWifi(data) {
    // init formated data
    var formatedData = {};
    formatedData.header = {};
    formatedData.data = {};

    // header
    formatedData.header.version = "1.2";
    formatedData.header.commandclass = "OPEN_DATA";
    formatedData.header.id = data.recordid;
    formatedData.header.timestamp = new Date(data.record_timestamp).getTime() / 1000;
    formatedData.header.attributes = [];
    formatedData.header.attributes[0] = {};
    formatedData.header.attributes[0].key = "type";
    formatedData.header.attributes[0].value = data.datasetid;

    // data
    formatedData.data.action = "set";
    formatedData.data.informationmap = [];
    // fields
    var i = 0;
    for(var key in data.fields) {
      if (Object.prototype.hasOwnProperty.call(data.fields, key)) {
        const val = String(data.fields[key]);
        formatedData.data.informationmap[i] = {};
        formatedData.data.informationmap[i].key = key;
        formatedData.data.informationmap[i].value = val;
        i++;
      }
    }

    return formatedData;
  }

  function convertOpenDataBusPositionRealTime(data) {
    // init formated data
    var formatedData = {};
    formatedData.header = {};
    formatedData.data = {};

    // header
    formatedData.header.version = "1.2";
    formatedData.header.commandclass = "OPEN_DATA";
    formatedData.header.id = data.recordid;
    formatedData.header.timestamp = new Date(data.record_timestamp).getTime() / 1000;
    formatedData.header.attributes = [];
    formatedData.header.attributes[0] = {};
    formatedData.header.attributes[0].key = "type";
    formatedData.header.attributes[0].value = data.datasetid;

    // data
    formatedData.data.action = "set";
    formatedData.data.informationmap = [];
    // fields
    var i = 0;
    for(var key in data.fields) {
      if (Object.prototype.hasOwnProperty.call(data.fields, key)) {
        const val = data.fields[key];
        if(key === "coordonnees") {
          // longitude
          formatedData.data.informationmap[i] = {};
          formatedData.data.informationmap[i].key = "latitude";
          formatedData.data.informationmap[i].value = String(val[0]);
          i++;
          // latitude
          formatedData.data.informationmap[i] = {};
          formatedData.data.informationmap[i].key = "longitude";
          formatedData.data.informationmap[i].value = String(val[1]);
        }
        else {
          formatedData.data.informationmap[i] = {};
          formatedData.data.informationmap[i].key = key;
          formatedData.data.informationmap[i].value = String(val);
        }
        i++;
      }
    }

    return formatedData;
  }

  RED.nodes.registerType("convertor", convertData);
}
