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
            break;

          case "GENERIC_FORMAT":
            formatedData = convertGenericFormat(data);
            break;
        }

        msg.payload = formatedData;
        if(!msg.payload.id)
          msg.payload.id = guid()
        msg.table = formatedData.table;
        msg.documentType = formatedData.table;
        msg.dataType = formatedData.elastic.header.attributes.type;
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
    // cassandra
    formatedData.cassandra = {};
    formatedData.cassandra.header = {};
    formatedData.cassandra.data = {};
    // new format
    formatedData.cassandraSensorValues = {};
    formatedData.cassandraSensorValues.header = {};
    formatedData.cassandraSensorValues.data = {};
    // elastic
    formatedData.elastic = {};
    formatedData.elastic.header = {};
    formatedData.elastic.data = {};

    // header
    // cassandra
    formatedData.cassandra.header.version = "1.2";
    formatedData.cassandra.header.commandclass = "OPEN_DATA";
    formatedData.cassandra.header.id = data.recordid;
    formatedData.cassandra.header.timestamp = new Date(data.record_timestamp).getTime() / 1000;
    formatedData.cassandra.header.attributes = [];
    formatedData.cassandra.header.attributes[0] = {};
    formatedData.cassandra.header.attributes[0].key = "type";
    formatedData.cassandra.header.attributes[0].value = data.datasetid;
    // elastic
    formatedData.elastic.header.version = "1.2";
    formatedData.elastic.header.commandclass = "OPEN_DATA";
    formatedData.elastic.header.id = data.recordid;
    formatedData.elastic.header.timestamp = new Date(data.record_timestamp).getTime() / 1000;
    formatedData.elastic.header.attributes = {};
    formatedData.elastic.header.attributes.type = data.datasetid;
    // new format
    formatedData.cassandraSensorValues.header = formatedData.elastic.header;

    // data
    formatedData.cassandra.data.action = "set";
    formatedData.cassandra.data.informationmap = [];
    formatedData.elastic.data = data.fields;
    // fields
    var i = 0;
    for(var key in data.fields) {
      if (Object.prototype.hasOwnProperty.call(data.fields, key)) {
        const val = String(data.fields[key]);
        formatedData.cassandra.data.informationmap[i] = {};
        formatedData.cassandra.data.informationmap[i].key = key;
        formatedData.cassandra.data.informationmap[i].value = val;
        i++;
        // new format
        formatedData.cassandraSensorValues.data[key] = val;
      }
    }

    // table
    formatedData.table = "opendata";
    return formatedData;
  }

  function convertOpenDataWithLocation(data) {
    // init formated data
    var formatedData = {};
    // cassandra
    formatedData.cassandraGenericFormat = {};
    formatedData.cassandraGenericFormat.header = {};
    formatedData.cassandraGenericFormat.data = {};
    // new format
    formatedData.cassandraSensorValues = {};
    formatedData.cassandraSensorValues.header = {};
    formatedData.cassandraSensorValues.data = {};
    // elastic
    formatedData.elastic = {};
    formatedData.elastic.header = {};
    formatedData.elastic.data = {};

    // header
    // cassandra
    formatedData.cassandraGenericFormat.header.version = "1.2";
    formatedData.cassandraGenericFormat.header.commandclass = "OPEN_DATA";
    formatedData.cassandraGenericFormat.header.id = data.recordid;
    formatedData.cassandraGenericFormat.header.timestamp = new Date(data.record_timestamp).getTime() / 1000;
    formatedData.cassandraGenericFormat.header.attributes = [];
    formatedData.cassandraGenericFormat.header.attributes[0] = {};
    formatedData.cassandraGenericFormat.header.attributes[0].key = "type";
    formatedData.cassandraGenericFormat.header.attributes[0].value = data.datasetid;
    // elastic
    formatedData.elastic.header.version = "1.2";
    formatedData.elastic.header.commandclass = "OPEN_DATA";
    formatedData.elastic.header.id = data.recordid;
    formatedData.elastic.header.timestamp = new Date(data.record_timestamp).getTime();
    formatedData.elastic.header.attributes = {};
    formatedData.elastic.header.attributes.type = data.datasetid;
    // new format
    formatedData.cassandraSensorValues.header = formatedData.elastic.header;

    // data
    formatedData.cassandraGenericFormat.data.action = "set";
    formatedData.cassandraGenericFormat.data.informationmap = [];
    formatedData.elastic.data = data.fields;
    if(formatedData.elastic.data.coordonnees) {
      const coordonnees = formatedData.elastic.data.coordonnees;
      formatedData.elastic.data.coordonnees = {};
      formatedData.elastic.data.coordonnees.lat = coordonnees[0];
      formatedData.elastic.data.coordonnees.lon = coordonnees[1];
    }
    // fields
    var i = 0;
    for(var key in data.fields) {
      if (Object.prototype.hasOwnProperty.call(data.fields, key)) {
        const val = data.fields[key];
        if(key === "coordonnees") {
          // longitude
          formatedData.cassandraGenericFormat.data.informationmap[i] = {};
          formatedData.cassandraGenericFormat.data.informationmap[i].key = "latitude";
          formatedData.cassandraGenericFormat.data.informationmap[i].value = String(val[0]);
          formatedData.cassandraSensorValues.data.latitude = String(val[0]);
          i++;
          // latitude
          formatedData.cassandraGenericFormat.data.informationmap[i] = {};
          formatedData.cassandraGenericFormat.data.informationmap[i].key = "longitude";
          formatedData.cassandraGenericFormat.data.informationmap[i].value = String(val[1]);
          formatedData.cassandraSensorValues.data.longitude = String(val[1]);
        }
        else {
          formatedData.cassandraGenericFormat.data.informationmap[i] = {};
          formatedData.cassandraGenericFormat.data.informationmap[i].key = key;
          formatedData.cassandraGenericFormat.data.informationmap[i].value = String(val);
          // new format
          formatedData.cassandraSensorValues.data[key] = String(val);
        }
        i++;
      }
    }

    // table
    formatedData.table = "opendata";
    return formatedData;
  }

  function convertSensorValue(data) {
    // init formated data
    var formatedData = {};
    // cassandra
    formatedData.cassandraGenericFormat = {};
    formatedData.cassandraGenericFormat.header = {};
    formatedData.cassandraGenericFormat.data = {};
    formatedData.cassandraSensorValues = {};
    formatedData.cassandraSensorValues.header = {};
    formatedData.cassandraSensorValues.data = {};
    // elastic
    formatedData.elastic = {};
    formatedData.elastic.header = {};
    formatedData.elastic.data = {};

    // header
    // cassandra
    formatedData.cassandraGenericFormat.header.version = "1.2";
    formatedData.cassandraGenericFormat.header.commandclass = data.commandclass;
    formatedData.cassandraGenericFormat.header.id = data.id;
    formatedData.cassandraGenericFormat.header.timestamp = data.date || new Date().getTime();
    formatedData.cassandraGenericFormat.header.attributes = [];
    formatedData.cassandraGenericFormat.header.attributes[0] = {};
    formatedData.cassandraGenericFormat.header.attributes[0].key = "type";
    formatedData.cassandraGenericFormat.header.attributes[0].value = data.type;
    // elastic
    formatedData.elastic.header.version = "1.2";
    formatedData.elastic.header.commandclass = data.commandclass;
    formatedData.elastic.header.id = data.id;
    formatedData.elastic.header.timestamp = data.date || new Date().getTime();
    formatedData.elastic.header.attributes = {};
    formatedData.elastic.header.attributes.type = data.type;

    // data
    // cassandra
    formatedData.cassandraGenericFormat.data.action = "set";
    formatedData.cassandraGenericFormat.data.informationmap = [];
    formatedData.cassandraGenericFormat.data.informationmap[0] = {};
    formatedData.cassandraGenericFormat.data.informationmap[0].key = "status";
    formatedData.cassandraGenericFormat.data.informationmap[0].value = String(data.value);
    // elastic
    formatedData.elastic.data.status = data.value;

    // table
    formatedData.table = "sensorvalues";
    return formatedData;
  }

  function convertGenericFormat(data) {
    // init formated data
    var formatedData = {};
    // cassandra
    formatedData.cassandraGenericFormat = data;
    formatedData.cassandraSensorValues = {};
    formatedData.cassandraSensorValues.header = {};
    formatedData.cassandraSensorValues.data = {};
    // elastic
    formatedData.elastic = {};
    formatedData.elastic.header = {};
    formatedData.elastic.data = {};

    // header
    // cassandra
    formatedData.cassandraSensorValues.header.version = "1.2";
    formatedData.cassandraSensorValues.header.commandclass = data.header.commandclass;
    formatedData.cassandraSensorValues.header.id = data.header.id;
    formatedData.cassandraSensorValues.header.timestamp = data.header.date || new Date().getTime();
    formatedData.cassandraSensorValues.header.attributes = {};
    formatedData.cassandraSensorValues.header.attributes.type = data.header.attributes[0].value;
    // elastic
    formatedData.elastic.header = formatedData.cassandraSensorValues.header;

    // data
    // cassandra
    for(var item of data.data.informationmap) {
      formatedData.cassandraSensorValues.data[item.key] = item.value;
    }
    // elastic
    formatedData.elastic.data = formatedData.cassandraSensorValues.data;

    // table
    formatedData.table = "sensorvalues";
    return formatedData;
  }

  function guid() {
    function s4() {
      return Math.floor((1 + Math.random()) * 0x10000)
        .toString(16)
        .substring(1);
    }
    return s4() + s4() + '-' + s4() + '-' + s4() + '-' +
      s4() + '-' + s4() + s4() + s4();
  }

  RED.nodes.registerType("convertor", convertData);
}
