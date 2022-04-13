const { Rest, path } = require('./rest');
const aliyunAccount = require('./auth');
const _ = require('lodash');

const DataHub = function (accessId, accessKey, endPoint, args) {
  args || (args = {});
  securityToken = args.securityToken || '';
  this.account = new aliyunAccount(accessId, accessKey, securityToken);
  this.endPoint = endPoint;
  this.rest = new Rest(this.account, this.endPoint, args);
};

DataHub.prototype.createProject = function (projectName, params, callback) {
  const urn = path.project(projectName);
  const requestParams = _.pick(params, ['Comment']);
  this.rest.post(urn, requestParams, callback);
};

DataHub.prototype.listProject = function (callback) {
  const urn = path.projects();
  this.rest.get(urn, callback);
};

DataHub.prototype.getProject = function (projectName, callback) {
  const urn = path.project(projectName);
  this.rest.get(urn, callback);
};

DataHub.prototype.updateProject = function (projectName, params, callback) {
  const urn = path.project(projectName);
  const requestParams = _.pick(params, ['Comment']);
  this.rest.put(urn, requestParams, callback);
};

DataHub.prototype.deleteProject = function (projectName, callback) {
  const urn = path.project(projectName);
  this.rest.delete(urn, callback);
};

DataHub.prototype.createTopic = function (projectName, topicName, params, callback) {
  const urn = path.topic(projectName, topicName);
  const requestParams = _.pick(params, ['Action', 'ShardCount', 'Lifecycle', 'RecordType', 'RecordSchema', 'Comment']);
  this.rest.post(urn, requestParams, callback);
};

DataHub.prototype.getTopic = function (projectName, topicName, callback) {
  const urn = path.topic(projectName, topicName);
  this.rest.get(urn, callback);
};

DataHub.prototype.listTopic = function (projectName, callback) {
  const urn = path.topics(projectName);
  this.rest.get(urn,callback);
};

DataHub.prototype.updateTopic = function (projectName, topicName, params, callback) {
  const urn = path.topic(projectName, topicName);
  const requestParams = _.pick(params, ['Comment']);
  this.rest.put(urn, requestParams, callback);
};

DataHub.prototype.deleteTopic = function (projectName, topicName, callback) {
  const urn = path.topic(projectName, topicName);
  this.rest.delete(urn, callback);
};

DataHub.prototype.listShard = function (projectName, topicName, callback) {
  const urn = path.shards(projectName, topicName);
  this.rest.get(urn, callback);
};

DataHub.prototype.splitShard = function (projectName, topicName, params, callback) {
  const urn = path.shards(projectName, topicName);
  const requestParams = _.pick(params, ['Action', 'ShardId', 'SplitKey']);
  this.rest.post(urn, requestParams, callback);
};

DataHub.prototype.mergeShard = function (projectName, topicName, params, callback) {
  const urn = path.shards(projectName, topicName);
  const requestParams = _.pick(params, ['Action', 'ShardId', 'AdjacentShardId']);
  this.rest.post(urn, requestParams, callback);
};

DataHub.prototype.getCursor = function (projectName, topicName, shardId, params, callback) {
  const urn = path.shard(projectName, topicName, shardId);
  const params = _.pick(params, ['Action', 'Type', 'SystemTime', 'Sequence']);
  let requestParams = {
    Action: params.Action || 'cursor',
    Type: params.Type || 'LATEST'
  };
  switch (params.type) {
    case 'SYSTEM_TIME':
      requestParams.SystemTime = params.SystemTime || new Date().getTime();
      break;
    case 'SEQUENCE':
      requestParams.Sequence = params.Sequence;
      break;
    default:
      break;
  }
  this.rest.post(urn, requestParams, callback);
};

DataHub.prototype.putTupleRecord = function (projectName, topicName, params, callback) {
  const urn = path.shards(projectName, topicName);
  const recordsParams = _.pick(params, ['Action', 'ShardId', 'Attributes', 'Data']);
  const requestParams = {
    Action: params.Action || 'pub',
    Records: [{
      ShardId: recordsParams.ShardId || '0',
      Attributes: recordsParams.Attributes || {a: 'b'},
      Data: recordsParams.Data
    }]
  };
  this.rest.post(urn, requestParams, callback);
};

DataHub.prototype.putBlobRecord = function (projectName, topicName, params, callback) {
  const urn = path.shards(projectName, topicName);
  const recordParams = _.pick(params, ['Action', 'ShardId', 'Attributes', 'Data']);
  const requestParams = {
    Action: params.Action || 'pub',
    Records: [{
      ShardId: recordParams.ShardId || '0',
      Attributes: recordParams.Attributes || {a: 'b'},
      Data: Buffer.from(recordParams.Data).toString('base64')
    }]
  };
  this.rest.post(urn, requestParams, callback);
};

DataHub.prototype.getRecords = function (projectName, topicName, shardId, params, callback) {
  const urn = path.shard(projectName, topicName, shardId);
  const requestParams = _.pick(params, ['Cursor', 'Limit']);
  requestParams.Action = 'sub';
  this.rest.post(urn, requestParams, callback);
};

DataHub.prototype.createField = function (projectName, topicName, params, callback) {
  const urn = path.topic(projectName, topicName);
  const requestParams = _.pick(params, ['Action', 'FieldName', 'FieldType']);
  this.rest.post(urn, requestParams, callback);
};

module.exports = DataHub;
