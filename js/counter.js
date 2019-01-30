/*
 * アトミックなカウンターのサンプル
 */

const AWS = require('aws-sdk');

async function main() {
  let updatedItem = await increment('aoyama_counter', { 'id': 'hoge' }, 1);
  console.log(updatedItem);
}

/**
  * @param {string} tableName
  * @param {Object<string, *>} key
  * @param {number} incr
  * @return {Promise.<DocumentClient.AttributeMap>}
  */
async function increment(tableName, key, incr) {
  const docClient = new AWS.DynamoDB.DocumentClient();
  /**
   *
   * @type {DocumentClient.UpdateItemInput}
   */
  const params = {
    TableName: tableName,
    Key: key,
    ReturnValues: 'ALL_NEW',
    UpdateExpression: 'SET #value = #value + :incr',
    ExpressionAttributeNames: {
      '#value': 'value'
    },
    ExpressionAttributeValues: {
      ':incr': incr
    }
  };
  console.log("INCREMENTAL_UPDATE", params);
  let res = await docClient.update(params).promise();
  return res.Attributes;
}

main();
