# FLYGPT PLUS API

本文档描述当前对外兑换接口。前台只领取已经存在于成品号池中的账号，不会启动注册、浏览器、支付或 Plus 激活任务。

## 兑换流程

```text
验证成品 CDK
    |
    v
从 product_assets 选择 status='正常' 且 shipped=0 的账号
    |
    v
同一事务内标记账号出库并核销 CDK
    |
    v
同步返回账号和可用下载项
```

兑换接口不返回 `jobKey`，前台也不需要 WebSocket。号池缺货时直接返回 `409`，不会现场生产账号。

## 通用约定

- Base URL: `http://localhost:3000`
- 请求和 JSON 响应使用 `application/json`
- 前台接口无需登录
- 错误响应格式：

```json
{
  "success": false,
  "message": "错误说明"
}
```

## 验证 CDK

`POST /api/verify-cdk`

仅检查 CDK 是否为未使用的成品 CDK，不核销、不出库。

请求：

```json
{
  "cdk": "XXXXXXXXXXXX"
}
```

成功响应：

```json
{
  "success": true,
  "data": {
    "type": "成品"
  }
}
```

常见错误：

- `400`: 未提供 CDK
- `403`: CDK 无效、已使用或不支持兑换
- `500`: 数据库或服务错误

## 领取成品账号

`POST /api/redeem-product`

从现有号池同步领取一个可用账号。服务端在 MySQL 事务中锁定 CDK 和账号，避免同一码或同一账号被重复领取。

请求：

```json
{
  "cdk": "XXXXXXXXXXXX"
}
```

成功响应：

```json
{
  "success": true,
  "completed": true,
  "message": "成品账号领取成功",
  "result": {
    "email": "account@example.com",
    "imapKey": "",
    "planType": "plus",
    "downloadAvailable": true,
    "sub2apiAvailable": true,
    "cpaAvailable": true
  }
}
```

字段说明：

| 字段 | 类型 | 说明 |
|---|---|---|
| `result.email` | string | 交付账号邮箱 |
| `result.imapKey` | string | 账号对应的 IMAP Key；未配置时为空字符串 |
| `result.planType` | string | `free` 或 `plus` |
| `result.downloadAvailable` | boolean | 是否存在任一可下载凭证 |
| `result.sub2apiAvailable` | boolean | 是否可下载 Sub2API 凭证 |
| `result.cpaAvailable` | boolean | 是否可下载 CPA 凭证 |

常见错误：

- `400`: 未提供 CDK
- `403`: CDK 无效、已使用或不是成品 CDK
- `409`: 成品号池缺货
- `503`: 系统维护中
- `500`: 数据库或服务错误

`403`、`409`、`503` 和 `500` 均不会触发账号生产。

## 查询 CDK

`GET /api/cdk/query?cdk=XXXXXXXXXXXX`

查询成品 CDK 的使用状态和已领取凭证信息。

成功响应示例：

```json
{
  "success": true,
  "data": {
    "status": "已使用",
    "type": "成品",
    "createdAt": "2026-08-05T10:00:00.000Z",
    "usedAt": "2026-08-05 18:05:00",
    "imapKey": "",
    "downloadAvailable": true,
    "downloadFileName": "account@example.com-plus.json",
    "sub2apiAvailable": true,
    "sub2apiFileName": "account@example.com-plus.json",
    "cpaAvailable": true,
    "cpaFileName": "account@example.com.json"
  }
}
```

`status` 只有 `未使用` 和 `已使用`。查询接口不会恢复或创建任务。

## 下载凭证

`GET /api/cdk/download?cdk=XXXXXXXXXXXX&kind=sub2api`

参数：

| 参数 | 必填 | 说明 |
|---|---|---|
| `cdk` | 是 | 已成功兑换的成品 CDK |
| `kind` | 否 | `sub2api`（默认）或 `cpa` |

只有已核销并关联成品账号的 CDK 可以下载。文件不存在时返回 `404`。

## 已下线接口

`POST /api/run-process`

该接口固定返回 `410 Gone`：

```json
{
  "success": false,
  "message": "该接口已下线，前台仅支持成品 CDK 兑换"
}
```

## 调用示例

```javascript
async function redeemProduct(cdk) {
  const response = await fetch('/api/redeem-product', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ cdk })
  });

  const result = await response.json();
  if (!response.ok) {
    throw new Error(result.message || '兑换失败');
  }
  return result.result;
}
```

```bash
curl -X POST http://localhost:3000/api/redeem-product \
  -H 'Content-Type: application/json' \
  -d '{"cdk":"XXXXXXXXXXXX"}'
```

## 后台生产

后台管理员仍可通过以下受保护接口预先补充号池：

- `POST /api/admin/products/generate`
- `POST /api/admin/products/resume`
- `POST /api/admin/products/generate-stop`

这些接口需要管理员 Bearer Token。后台生产和前台兑换是两条独立路径；只有管理员接口能启动生产任务。
