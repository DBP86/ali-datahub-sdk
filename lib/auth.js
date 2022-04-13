const  { commonHeaders } = require('./rest');
const crypto = require('crypto');

const getCanonicalizedDataHubHeaders = (requestHeaders) => {
  let canonicalizedDataHubHeaders = `x-datahub-client-version:${requestHeaders['x-datahub-client-version']}`;

  for (let commonHeader in commonHeaders) {
    if (commonHeader === 'clientVersion') continue;
    const cHeader = commonHeaders[commonHeader];
    const requestHeader = requestHeaders[commonHeaders[commonHeader]];
    if (/x-datahub/.test(cHeader) && requestHeader) {
      canonicalizedDataHubHeaders += '\n' + cHeader + ':' + requestHeader;
    }
  }

  return canonicalizedDataHubHeaders;
};

const getCanonicalizedResource = (urn, requestParams) => {
  let canonicalizedResource = '';
  canonicalizedResource += urn;
  return canonicalizedResource;
};

const aliyunAccount = function (accessId, accessKey, securityToken) {
  this.accessId = accessId;
  this.accessKey = accessKey;
  this.securityToken = securityToken;
};


aliyunAccount.prototype.getSign = function (method, urn, requestHeaders, requestParams) {
  const canonicalizedDataHubHeaders = getCanonicalizedDataHubHeaders(requestHeaders);
  const canonicalizedResource = getCanonicalizedResource(urn, requestParams);
  const sign = method + '\n' + requestHeaders['Content-Type'] + '\n' + requestHeaders['Date'] + '\n' + canonicalizedDataHubHeaders + '\n' + canonicalizedResource;
  const signature = crypto.createHmac('sha1', this.accessKey).update(sign).digest().toString('base64');
  const Authorization = 'DATAHUB ' + this.accessId + ':' + signature;
  return Authorization;
};

module.exports = aliyunAccount;
