const { clientVersion } = require('./version');
const request = require('request');

const HTTPMethod = {
  GET: 'GET',
  POST: 'POST',
  PUT: 'PUT',
  HEAD: 'HEAD',
  DELETE: 'DELETE'
};

const contentType = {
  HTTPJson: 'application/json',
  HTTPProtobuf: 'application/x-protobuf'
};

const commonHeaders = {
  acceptEncoding: 'Accept-Encoding',
  Authorization: 'Authorization',
  cacheControl: 'Cache-Control',
  chunked: 'chunked',
  clientVersion: 'x-datahub-client-version',
  contentDisposition: 'Content-Disposition',
  contentEncoding: 'Content-Encoding',
  contentLength: 'Content-Length',
  contentMD5: 'Content-MD5',
  contentType: 'contentType',
  date: 'Date',
  ETag: 'ETag',
  expires: 'Expires',
  host: 'Host',
  lastModified: 'Last-Modified',
  location: 'Location',
  range: 'Range',
  rawSize: 'x-datahub-content-raw-size',
  requestAction: 'x-datahub-request-action',
  requestId: 'x-datahub-request-id',
  securityToken: 'x-datahub-security-token',
  transferEncoding: 'Transfer-Encoding',
  userAgent: 'User-Agent'
};

const path = {
  projects: () => {
    return '/projects';
  },
  project: (projectName) => {
    return '/projects/' + projectName;
  },
  topics: (projectName) => {
    return '/projects/' + projectName + '/topics';
  },
  topic: (projectName, topicName) => {
    return '/projects/' + projectName + '/topics/' + topicName;
  },
  shards: (projectName, topicName) => {
    return '/projects/' + projectName + '/topics/' + topicName + '/shards';
  },
  shard: (projectName, topicName, shardId) => {
    return '/projects/' + projectName + '/topics/' + topicName + '/shards/' + shardId;
  },
  connectors: (projectName, topicName) => {
    return '/projects/' + projectName + '/topics/' + topicName + '/connectors';
  },
  connector: (projectName, topicName, ConnectorType) => {
    return '/projects/' + projectName + '/topics/' + topicName + '/connectors/' + ConnectorType;
  },
  done_time: (projectName, topicName, ConnectorType) => {
    return '/projects/' + projectName + '/topics/' + topicName + '/connectors/' + ConnectorType + '?donetime';
  },
  subscriptions: (projectName, topicName) => {
    return '/projects/' + projectName + '/topics/' + topicName + '/subscriptions';
  },
  subscription: (projectName, topicName, subscriptions) => {
    return '/projects/' + projectName + '/topics/' + topicName + '/subscriptions/' + subscriptions;
  },
  offsets: (projectName, topicName, subscriptions) => {
    return '/projects/' + projectName + '/topics/' + topicName + '/subscriptions/' + subscriptions + '/offsets';
  }
};

const Rest = function (account, endPoint) {
  this.account = account;
  this.endPoint = endPoint;
};

Rest.prototype.getHeaders = function () {
  let headers = {
    'x-datahub-client-version': clientVersion,
    'Date': new Date().toUTCString(),
    'Content-Type': contentType.HTTPJson
  };
  if (this.account.securityToken) headers['x-datahub-security-token'] = this.account.securityToken;
  return headers;
};

Rest.prototype.request = function (method, urn, params, callback) {
  let headers = this.getHeaders();
  const Authorization = this.account.getSign(method, urn, headers, params);
  headers.Authorization = Authorization;
  option = {
    url: this.endPoint + urn,
    method: method,
    headers: headers,
    rejectUnauthorized: false
  };
  if (params) option.json = params;
  request(option, (err, res, body) => {
    if (err) return callback(err);
    if (body && body.ErrorCode) return callback(body);
    callback(null, body);
  })
};

Rest.prototype.post = function (urn, params, callback) {
  this.request(HTTPMethod.POST, urn, params, callback);
};

Rest.prototype.delete = function (urn, callback) {
  this.request(HTTPMethod.DELETE, urn, null, callback);
};

Rest.prototype.put = function (urn, params, callback) {
  this.request(HTTPMethod.PUT, urn, params, callback);
};

Rest.prototype.get = function (urn, callback) {
  this.request(HTTPMethod.GET, urn, null, callback);
};

module.exports = {
  HTTPMethod: HTTPMethod,
  contentType: contentType,
  commonHeaders: commonHeaders,
  path: path,
  Rest: Rest
};
