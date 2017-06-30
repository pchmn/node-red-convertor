module.exports = function(RED) {
  "use strict";

  function convertData(config) {
      RED.nodes.createNode(this, config);
      var node = this;

      node.on('input', function(msg) {
        // get data type
        const dataType = msg.req ? msg.req.headers["x-data-type"] : msg.dataType;
        // get data
        var data = {};
        data = msg.payload;
        // result
        var formatedData = {};

        switch(dataType) {
          case "OPEN_DATA_WITHOUT_LOCATION": 
            formatedData = convertOpenDataWithoutLocation(data);
            break;

          case "OPEN_DATA_WITH_LOCATION": 
            formatedData = convertOpenDataWithLocation(data);
            break;

          case "SENSOR":
            formatedData = convertSensorValue(data);
        }

        msg.payload = formatedData;
        msg.dataType = formatedData.header.attributes[0].value;
        node.send(msg);
      });
  }

  /**
   * [convertOpenDataWifi description]
   * @param  {[type]} data [description]
   * @return {[type]}      [description]
   */
  function convertOpenDataWithoutLocation(data) {
    // init formated data
    var formatedData = {};
    formatedData.header = {};
    formatedData.data = {};

    // header
    formatedData.header.version = "1.2";
    formatedData.header.commandclass = "OPEN_DATA";
    formatedData.header.id = data.recordid;
    formatedData.header.timestamp = new Date(data.record_timestamp).getTime();
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

  function convertOpenDataWithLocation(data) {
    // init formated data
    var formatedData = {};
    formatedData.header = {};
    formatedData.data = {};

    // header
    formatedData.header.version = "1.2";
    formatedData.header.commandclass = "OPEN_DATA";
    formatedData.header.id = data.recordid;
    formatedData.header.timestamp = new Date(data.record_timestamp).getTime();
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

  function convertSensorValue(data) {
    // init formated data
    var formatedData = {};
    formatedData.header = {};
    formatedData.data = {};

    // header
    formatedData.header.version = "1.2";
    formatedData.header.commandclass = data.commandclass;
    formatedData.header.id = data.id;
    formatedData.header.timestamp = data.date || new Date().getTime();
    formatedData.header.attributes = [];
    formatedData.header.attributes[0] = {};
    formatedData.header.attributes[0].key = "type";
    formatedData.header.attributes[0].value = data.type;

    // data
    formatedData.data.action = "set";
    formatedData.data.informationmap = [];
    formatedData.data.informationmap[0] = {};
    formatedData.data.informationmap[0].key = "status";
    formatedData.data.informationmap[0].value = String(data.value);

    return formatedData;
  }

  RED.nodes.registerType("convertor", convertData);
}
