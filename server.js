require('./load-env');

const express = require('express');
const { execFileSync } = require('child_process');
const path = require('path');
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const WebSocket = require('ws');
const axios = require('axios');
const store = require('./mysql-store');
const { listRecentEmailsForAdmin } = require('./pool-email-imap');
const runtimeLog = require('./runtime-log');
const { initializeImapAuth, getImapAuthHeaders } = require('./imap-auth');

const app = express();
const PORT = Number(process.env.PORT || 3000);
const ADMIN_TOKEN_TTL_MS = 24 * 60 * 60 * 1000;
const ADMIN_REFRESH_AFTER_MS = 60 * 60 * 1000;
const WS_HEARTBEAT_PING_TYPE = 'ping';
const WS_HEARTBEAT_PONG_TYPE = 'pong';
const ACCESS_DEACTIVATED_MESSAGES_URL = String(
    process.env.ACCESS_DEACTIVATED_MESSAGES_URL
        || ''
).trim();
const ACCESS_DEACTIVATED_SYNC_KEY = 'access_deactivated_messages_last_since';
const ACCESS_DEACTIVATED_SYNC_COOLDOWN_MS = 30 * 1000;
const ACCESS_DEACTIVATED_SYNC_MAX_BACKOFF_MS = 10 * 60 * 1000;
const ADMIN_TOKEN_SECRET = process.env.ADMIN_TOKEN_SECRET || crypto
    .createHash('sha256')
    .update(`web_redeem:${process.cwd()}:admin-token-secret`)
    .digest('hex');

// WebSocket 客户端映射: jobKey -> Set<WebSocket>
const taskClients = new Map();
const TERMINAL_TASK_STATUSES = new Set(['success', 'failed', 'maintenance']);
const activeForegroundJobs = new Set();
const activeBackgroundJobs = new Set();

/** 后台成品批量生产：job_key -> 停止回调（将批次 aborted 置 true） */
const adminGenerationStopHandlers = new Map();

function registerAdminGenerationStop(jobKey, fn) {
    adminGenerationStopHandlers.set(String(jobKey), fn);
}

function unregisterAdminGenerationStop(jobKey) {
    adminGenerationStopHandlers.delete(String(jobKey));
}

function requestAdminGenerationStop(jobKey) {
    const fn = adminGenerationStopHandlers.get(String(jobKey));
    if (typeof fn !== 'function') {
        return false;
    }
    try {
        fn();
    } catch (_) {
        /* ignore */
    }
    return true;
}

let systemMetricsCache = {
    ts: 0,
    data: null,
    promise: null
};
let accessDeactivatedSyncPromise = null;
let accessDeactivatedLastSyncAt = 0;
let accessDeactivatedSyncTimer = null;
let accessDeactivatedSyncFailureCount = 0;
let accessDeactivatedLastSyncError = '';

function reserveBackgroundSlot(slotKey) {
    activeBackgroundJobs.add(String(slotKey));
}

function releaseBackgroundSlot(slotKey) {
    activeBackgroundJobs.delete(String(slotKey));
}

function getTotalActiveJobs() {
    return activeForegroundJobs.size + activeBackgroundJobs.size;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseFlexibleTimestamp(value) {
    if (value == null || value === '') {
        return null;
    }

    if (value instanceof Date) {
        const time = value.getTime();
        return Number.isFinite(time) ? time : null;
    }

    if (typeof value === 'number' && Number.isFinite(value)) {
        if (value > 1e12) {
            return Math.trunc(value);
        }
        if (value > 1e9) {
            return Math.trunc(value * 1000);
        }
        return null;
    }

    const normalized = String(value).trim();
    if (!normalized) {
        return null;
    }

    if (/^\d{13}$/.test(normalized)) {
        return Number(normalized);
    }
    if (/^\d{10}$/.test(normalized)) {
        return Number(normalized) * 1000;
    }

    const candidate = normalized.includes('T')
        ? normalized
        : normalized.replace(' ', 'T');
    const parsed = Date.parse(candidate);
    if (Number.isFinite(parsed)) {
        return parsed;
    }

    return null;
}

function collectMessageEmails(message) {
    const emails = new Set();

    const addEmail = (value) => {
        const normalized = String(value || '').trim().toLowerCase();
        if (normalized && normalized.includes('@')) {
            emails.add(normalized);
        }
    };

    addEmail(message?.targetRecipient);

    const recipientList = String(message?.recipientList || '');
    const matches = recipientList.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/ig) || [];
    for (const email of matches) {
        addEmail(email);
    }

    return [...emails];
}

async function syncAccessDeactivatedProductStatuses(force = false) {
    if (!ACCESS_DEACTIVATED_MESSAGES_URL) {
        return { skipped: true, reason: 'disabled' };
    }

    const now = Date.now();
    if (!force && accessDeactivatedSyncPromise) {
        return accessDeactivatedSyncPromise;
    }
    if (!force && (now - accessDeactivatedLastSyncAt) < ACCESS_DEACTIVATED_SYNC_COOLDOWN_MS) {
        return { skipped: true, reason: 'cooldown' };
    }

    accessDeactivatedSyncPromise = (async () => {
        const previousSinceValue = await store.getAppConfigValue(ACCESS_DEACTIVATED_SYNC_KEY, '');
        const previousSinceTs = parseFlexibleTimestamp(previousSinceValue);
        const params = {};
        if (previousSinceTs) {
            params.since = String(previousSinceTs);
        }

        try {
            const response = await axios.get(ACCESS_DEACTIVATED_MESSAGES_URL, {
                headers: await getImapAuthHeaders(),
                params,
                timeout: 30000
            });

            const messages = Array.isArray(response?.data?.messages) ? response.data.messages : [];
            const emailSet = new Set();
            let latestMessageTs = previousSinceTs;

            for (const message of messages) {
                for (const email of collectMessageEmails(message)) {
                    emailSet.add(email);
                }
                const messageTs = parseFlexibleTimestamp(message?.date);
                if (messageTs && (!latestMessageTs || messageTs > latestMessageTs)) {
                    latestMessageTs = messageTs;
                }
            }

            const affectedRows = await store.updateProductStatusByEmails([...emailSet], '封禁');
            const nextSinceTs = latestMessageTs || now;
            await store.setAppConfigValue(ACCESS_DEACTIVATED_SYNC_KEY, String(nextSinceTs));
            accessDeactivatedLastSyncAt = Date.now();
            accessDeactivatedSyncFailureCount = 0;
            accessDeactivatedLastSyncError = '';

            if (messages.length > 0 || affectedRows > 0) {
                console.log(`[AccessDeactivated] synced messages=${messages.length} matchedEmails=${emailSet.size} updatedProducts=${affectedRows} since=${previousSinceTs || 'all'} nextSince=${nextSinceTs}`);
            }

            return {
                messagesCount: messages.length,
                matchedEmails: emailSet.size,
                updatedProducts: affectedRows,
                previousSinceTs,
                nextSinceTs
            };
        } catch (error) {
            const isUnauthorized = Number(error?.response?.status || 0) === 401;
            if (isUnauthorized) {
                try {
                    const retryResponse = await axios.get(ACCESS_DEACTIVATED_MESSAGES_URL, {
                        headers: await getImapAuthHeaders(true),
                        params,
                        timeout: 30000
                    });

                    const retryMessages = Array.isArray(retryResponse?.data?.messages) ? retryResponse.data.messages : [];
                    const retryEmailSet = new Set();
                    let latestMessageTs = previousSinceTs;

                    for (const message of retryMessages) {
                        for (const email of collectMessageEmails(message)) {
                            retryEmailSet.add(email);
                        }
                        const messageTs = parseFlexibleTimestamp(message?.date);
                        if (messageTs && (!latestMessageTs || messageTs > latestMessageTs)) {
                            latestMessageTs = messageTs;
                        }
                    }

                    const affectedRows = await store.updateProductStatusByEmails([...retryEmailSet], '封禁');
                    const nextSinceTs = latestMessageTs || now;
                    await store.setAppConfigValue(ACCESS_DEACTIVATED_SYNC_KEY, String(nextSinceTs));
                    accessDeactivatedLastSyncAt = Date.now();
                    accessDeactivatedSyncFailureCount = 0;
                    accessDeactivatedLastSyncError = '';

                    if (retryMessages.length > 0 || affectedRows > 0) {
                        console.log(`[AccessDeactivated] synced after token refresh messages=${retryMessages.length} matchedEmails=${retryEmailSet.size} updatedProducts=${affectedRows} since=${previousSinceTs || 'all'} nextSince=${nextSinceTs}`);
                    }

                    return {
                        messagesCount: retryMessages.length,
                        matchedEmails: retryEmailSet.size,
                        updatedProducts: affectedRows,
                        previousSinceTs,
                        nextSinceTs
                    };
                } catch (retryError) {
                    const errorMessage = String(retryError?.message || retryError);
                    accessDeactivatedSyncFailureCount += 1;
                    if (errorMessage !== accessDeactivatedLastSyncError) {
                        console.error(`[AccessDeactivated] sync failed after token refresh: ${errorMessage}`);
                        accessDeactivatedLastSyncError = errorMessage;
                    }
                    return {
                        skipped: true,
                        reason: 'unavailable',
                        error: errorMessage
                    };
                }
            }

            const errorMessage = String(error?.message || error);
            accessDeactivatedSyncFailureCount += 1;
            if (errorMessage !== accessDeactivatedLastSyncError) {
                console.error(`[AccessDeactivated] sync failed: ${errorMessage}`);
                accessDeactivatedLastSyncError = errorMessage;
            }
            return {
                skipped: true,
                reason: 'unavailable',
                error: errorMessage
            };
        } finally {
            accessDeactivatedSyncPromise = null;
        }
    })();

    return accessDeactivatedSyncPromise;
}

function scheduleAccessDeactivatedSync(delayMs = ACCESS_DEACTIVATED_SYNC_COOLDOWN_MS) {
    if (!ACCESS_DEACTIVATED_MESSAGES_URL) {
        return;
    }

    if (accessDeactivatedSyncTimer) {
        clearTimeout(accessDeactivatedSyncTimer);
        accessDeactivatedSyncTimer = null;
    }

    accessDeactivatedSyncTimer = setTimeout(async () => {
        let nextDelay = ACCESS_DEACTIVATED_SYNC_COOLDOWN_MS;
        try {
            await ensureStoreReady();
            const result = await syncAccessDeactivatedProductStatuses(true);
            if (result?.reason === 'unavailable') {
                nextDelay = Math.min(
                    ACCESS_DEACTIVATED_SYNC_MAX_BACKOFF_MS,
                    ACCESS_DEACTIVATED_SYNC_COOLDOWN_MS * (2 ** Math.min(accessDeactivatedSyncFailureCount - 1, 5))
                );
                console.warn(`[AccessDeactivated] scheduled sync unavailable; retrying in ${Math.ceil(nextDelay / 1000)}s`);
            }
        } catch (error) {
            nextDelay = Math.min(
                ACCESS_DEACTIVATED_SYNC_MAX_BACKOFF_MS,
                ACCESS_DEACTIVATED_SYNC_COOLDOWN_MS * (2 ** Math.min(accessDeactivatedSyncFailureCount - 1, 5))
            );
            console.warn(`[AccessDeactivated] scheduled sync failed; retrying in ${Math.ceil(nextDelay / 1000)}s`);
        } finally {
            scheduleAccessDeactivatedSync(nextDelay);
        }
    }, Math.max(1000, Number(delayMs) || ACCESS_DEACTIVATED_SYNC_COOLDOWN_MS));
}

async function waitForAvailableActivationSlot(jobSet, maxConcurrentActivations, excludedSlotKeys = []) {
    const excluded = new Set((excludedSlotKeys || []).map((item) => String(item)));
    while (true) {
        let occupied = 0;
        for (const slot of jobSet) {
            if (!excluded.has(String(slot))) {
                occupied += 1;
            }
        }
        if (occupied < Math.max(1, Number(maxConcurrentActivations) || 1)) {
            return;
        }
        await sleep(1000);
    }
}

function isFatalProductGenerationError(error) {
    const message = String(error?.message || error || '');

    // 这些是「换代理 / 换号能解决」的临时性问题，绝对不算致命，要让父进程重试
    const transientRetry =
        message.includes('OpenAI 鉴权服务异常')
        || message.includes('代理或网络持续超时')
        || message.includes('浏览器连接被代理多次关闭')
        || message.includes('user_already_exists')
        || message.includes('该邮箱已被注册')
        || message.includes('个人资料表单校验失败');
    if (transientRetry) {
        return false;
    }

    const proxyFatal =
        message.includes('代理连接失败')
        || message.includes('代理认证失败')
        || message.includes('代理响应异常')
        || message.includes('代理不可用')
        || message.includes('账号余额');
    return message.includes('系统维护中')
        || message.includes('余额不足')
        || message.includes('资产池枯竭')
        || proxyFatal
        || message.includes('无法获取有效的 Access Token')
        || message.includes('页面仍无法正常显示')
        // 支付已成功但协议提取失败：终止整个 batch（避免再扣费），管理员后台手动补 RT 即可
        || message.includes('支付已成功但协议提取失败')
        || message.includes('支付已成功并占位入库');
}

function formatGigabytes(bytes) {
    return `${(Math.max(0, Number(bytes) || 0) / (1024 ** 3)).toFixed(1)}G`;
}

function formatDuration(ms) {
    const totalSeconds = Math.max(0, Math.floor((Number(ms) || 0) / 1000));
    const days = Math.floor(totalSeconds / 86400);
    const hours = Math.floor((totalSeconds % 86400) / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    if (days > 0) {
        return `${days}天 ${hours}时 ${minutes}分`;
    }
    if (hours > 0) {
        return `${hours}时 ${minutes}分 ${seconds}秒`;
    }
    if (minutes > 0) {
        return `${minutes}分 ${seconds}秒`;
    }
    return `${seconds}秒`;
}

function getCpuTimesSnapshot() {
    return os.cpus().map((cpu) => {
        const times = cpu.times || {};
        const idle = Number(times.idle || 0);
        const total = Object.values(times).reduce((sum, value) => sum + Number(value || 0), 0);
        return { idle, total };
    });
}

async function getCpuUsagePercent(sampleMs = 200) {
    const start = getCpuTimesSnapshot();
    await sleep(sampleMs);
    const end = getCpuTimesSnapshot();

    let idleDiff = 0;
    let totalDiff = 0;
    for (let index = 0; index < Math.min(start.length, end.length); index += 1) {
        idleDiff += Math.max(0, end[index].idle - start[index].idle);
        totalDiff += Math.max(0, end[index].total - start[index].total);
    }

    if (totalDiff <= 0) {
        return 0;
    }

    return Math.max(0, Math.min(100, Math.round((1 - (idleDiff / totalDiff)) * 100)));
}

function getDiskMetrics() {
    const cwdRootWithSlash = path.parse(process.cwd()).root || 'C:\\';
    const driveLetter = cwdRootWithSlash.replace(/[\\\/]+$/, '') || 'C:';
    const buildDiskMetrics = (totalBytes, freeBytes) => {
        const safeTotalBytes = Math.max(0, Number(totalBytes) || 0);
        const safeFreeBytes = Math.max(0, Number(freeBytes) || 0);
        const usedBytes = Math.max(0, safeTotalBytes - safeFreeBytes);
        const percent = safeTotalBytes > 0
            ? Math.max(0, Math.min(100, Math.round((usedBytes / safeTotalBytes) * 100)))
            : 0;

        return {
            drive: driveLetter,
            usedBytes,
            totalBytes: safeTotalBytes,
            percent,
            usedText: formatGigabytes(usedBytes),
            totalText: formatGigabytes(safeTotalBytes)
        };
    };

    try {
        if (typeof fs.statfsSync === 'function') {
            const stats = fs.statfsSync(cwdRootWithSlash);
            const blockSize = Math.max(0, Number(stats.bsize) || 0);
            const totalBlocks = Math.max(0, Number(stats.blocks) || 0);
            const freeBlocks = Math.max(0, Number(stats.bfree ?? stats.bavail) || 0);
            const totalBytes = blockSize * totalBlocks;
            const freeBytes = blockSize * freeBlocks;

            if (totalBytes > 0) {
                return buildDiskMetrics(totalBytes, freeBytes);
            }
        }
    } catch (_) {
    }

    try {
        const script = `(Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='${driveLetter}'" | Select-Object Size,FreeSpace) | ConvertTo-Json -Compress`;
        const output = execFileSync('powershell', ['-NoProfile', '-Command', script], {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore'],
            timeout: 4000
        }).trim();

        if (!output) {
            return null;
        }

        const parsed = JSON.parse(output);
        return buildDiskMetrics(parsed.Size, parsed.FreeSpace);
    } catch (_) {
        return null;
    }
}

async function getSystemMetrics() {
    const now = Date.now();
    if (systemMetricsCache.data && (now - systemMetricsCache.ts) < 5000) {
        return systemMetricsCache.data;
    }
    if (systemMetricsCache.promise) {
        return systemMetricsCache.promise;
    }

    systemMetricsCache.promise = (async () => {
        const totalMem = os.totalmem();
        const freeMem = os.freemem();
        const usedMem = Math.max(0, totalMem - freeMem);
        const memoryPercent = totalMem > 0 ? Math.max(0, Math.min(100, Math.round((usedMem / totalMem) * 100))) : 0;
        const cpuPercent = await getCpuUsagePercent();
        const disk = getDiskMetrics();

        const data = {
            cpu: {
                percent: cpuPercent,
                text: `${cpuPercent}%`
            },
            memory: {
                percent: memoryPercent,
                usedBytes: usedMem,
                totalBytes: totalMem,
                usedText: formatGigabytes(usedMem),
                totalText: formatGigabytes(totalMem),
                text: `${formatGigabytes(usedMem)}/${formatGigabytes(totalMem)}`
            },
            disk: disk || {
                drive: path.parse(process.cwd()).root.replace(/[\\\/]+$/, '') || 'C:',
                percent: 0,
                usedBytes: 0,
                totalBytes: 0,
                usedText: '0.0G',
                totalText: '0.0G'
            },
            uptime: {
                seconds: Math.max(0, Math.floor(process.uptime())),
                text: formatDuration(process.uptime() * 1000)
            }
        };

        systemMetricsCache = {
            ts: Date.now(),
            data,
            promise: null
        };

        return data;
    })().catch((error) => {
        systemMetricsCache.promise = null;
        throw error;
    });

    return systemMetricsCache.promise;
}

async function startAdminProductGenerationTask(count, options = {}) {
    const targetCount = Math.max(1, Math.min(Number(count) || 1, 100));
    const planType = String(options.planType || 'plus').trim().toLowerCase() === 'free' ? 'free' : 'plus';
    const maxConcurrentActivations = await store.getMaxBackgroundConcurrent();
    const workerCount = Math.min(targetCount, Math.max(1, maxConcurrentActivations));
    const resumeFrom = options.resumeFrom || null;
    const task = await store.createTaskLog({
        tokenPreview: 'ADMIN_PRODUCT_GEN',
        cdkCode: `ADMIN_PRODUCT_GEN:${targetCount}`,
        status: 'running',
        progress: 1
    });

    const itemProgress = new Map();
    let completed = 0;
    let successCount = 0;
    let failedCount = 0;
    let nextIndex = 1;
    let lastError = '';
    let lastProgress = 1;
    let aborted = false;

    registerAdminGenerationStop(task.jobKey, () => {
        aborted = true;
        logTask(task.jobKey, '管理员请求停止：本批次不再排队新的成品生产（当前正在执行的条次会尽快在步骤间隙退出）', 'warn');
    });

    const buildGenerationSummary = () => JSON.stringify({
        kind: 'admin_product_generation',
        targetCount,
        completedCount: completed,
        successCount,
        failedCount,
        workerCount,
        aborted,
        lastError,
        resumedFromJobKey: resumeFrom?.jobKey || null,
        resumedFromTargetCount: resumeFrom?.targetCount || null,
        resumedFromCompletedCount: resumeFrom?.completedCount || null,
        planType
    });

    const computeBatchProgress = () => {
        const inFlightProgress = Array.from(itemProgress.values()).reduce((sum, value) => sum + Math.max(0, Math.min(99, Number(value) || 0)), 0);
        const raw = ((completed * 100) + inFlightProgress) / targetCount;
        return Math.max(lastProgress, Math.min(99, Math.floor(raw)));
    };

    const publishBatchProgress = async (message) => {
        const progress = computeBatchProgress();
        lastProgress = progress;
        await store.updateTaskLog(task.jobKey, {
            status: 'running',
            message,
            rawOutput: buildGenerationSummary(),
            cdkCode: `ADMIN_PRODUCT_GEN:${targetCount}`,
            progress
        });
        broadcastToTask(task.jobKey, {
            type: 'progress',
            jobKey: task.jobKey,
            progress,
            status: 'running',
            message
        });
    };

    logTask(task.jobKey, `🎬 后台成品生产启动  count=${targetCount}  workerCount=${workerCount}${resumeFrom ? `  resumeFrom=${resumeFrom.jobKey}` : ''}`);

    (async () => {
        const worker = async () => {
            while (true) {
                if (aborted) {
                    return;
                }
                const currentIndex = nextIndex;
                if (currentIndex > targetCount) {
                    return;
                }
                nextIndex += 1;

                const slotKey = `${task.jobKey}:item:${currentIndex}`;
                itemProgress.set(currentIndex, 0);

                try {
                    let produced = false;
                    let attempt = 0;

                    while (!produced) {
                        if (aborted) {
                            return;
                        }

                        attempt += 1;
                        itemProgress.set(currentIndex, 1);
                        await publishBatchProgress(
                            activeBackgroundJobs.size >= maxConcurrentActivations
                                ? `第 ${currentIndex}/${targetCount} 个正在排队等待空闲并发槽位...`
                                : `正在生产第 ${currentIndex}/${targetCount} 个成品号...`
                        );

                        await waitForAvailableActivationSlot(activeBackgroundJobs, maxConcurrentActivations);
                        reserveBackgroundSlot(slotKey);

                        try {
                            await publishBatchProgress(`正在生产第 ${currentIndex}/${targetCount} 个成品号 (尝试 ${attempt})...`);
                            const result = await startProductCreation('', async (progressData) => {
                                itemProgress.set(currentIndex, Math.max(0, Math.min(99, Number(progressData.progress) || 0)));
                                const itemMessage = progressData.message || `正在生产第 ${currentIndex}/${targetCount} 个成品号...`;
                                await publishBatchProgress(`第 ${currentIndex}/${targetCount} 个: ${itemMessage}`);
                            }, { jobKey: task.jobKey, planType });

                            if (result?.success) {
                                await store.addProduct(
                                    result.email,
                                    result.sub2apiPath || result.sub2apiFile || '',
                                    null,
                                    result.accessToken || null,
                                    result.imapKey || null,
                                    planType
                                );
                                successCount += 1;
                                produced = true;
                                logTask(task.jobKey, `第 ${currentIndex}/${targetCount} 个成品号生产成功 email=${result.email}`);
                            } else {
                                throw new Error('未返回成功结果');
                            }
                        } catch (error) {
                            lastError = error.message || '未知错误';
                            if (isFatalProductGenerationError(error)) {
                                failedCount += 1;
                                aborted = true;
                                logTask(task.jobKey, `第 ${currentIndex}/${targetCount} 个遇到致命错误，任务终止: ${lastError}`, 'error');
                                throw error;
                            }
                            logTask(task.jobKey, `第 ${currentIndex}/${targetCount} 个非致命失败，准备重试: ${lastError}`, 'warn');
                            itemProgress.set(currentIndex, 1);
                            await publishBatchProgress(`第 ${currentIndex}/${targetCount} 个失败重试中: ${lastError}`);
                            await sleep(3000);
                        } finally {
                            releaseBackgroundSlot(slotKey);
                        }
                    }
                } finally {
                    if (!aborted) {
                        completed += 1;
                        itemProgress.delete(currentIndex);
                        await publishBatchProgress(
                            completed >= targetCount
                                ? '生产任务收尾中...'
                                : `已完成 ${completed}/${targetCount}，继续生产剩余成品号...`
                        );
                    }
                }
            }
        };

        try {
            await Promise.all(Array.from({ length: workerCount }, () => worker()));

            const finalStatus = aborted || failedCount > 0 ? 'failed' : 'success';
            const finalMessage = finalStatus === 'success'
                ? `成功生产 ${successCount} 个成品号`
                : `生产中止，已成功 ${successCount} 个${lastError ? `，原因：${lastError}` : ''}`;
            const finalProgress = 100;

            await store.updateTaskLog(task.jobKey, {
                status: finalStatus,
                message: finalMessage,
                rawOutput: buildGenerationSummary(),
                cdkCode: `ADMIN_PRODUCT_GEN:${targetCount}`,
                progress: finalProgress
            });
            broadcastToTask(task.jobKey, {
                type: 'status',
                jobKey: task.jobKey,
                progress: finalProgress,
                status: finalStatus,
                message: finalMessage
            });
        } catch (error) {
            lastError = error.message || lastError || '未知错误';
            const finalMessage = `后台成品生产异常: ${lastError}`;
            await store.updateTaskLog(task.jobKey, {
                status: 'failed',
                message: finalMessage,
                rawOutput: buildGenerationSummary(),
                cdkCode: `ADMIN_PRODUCT_GEN:${targetCount}`,
                progress: 100
            });
            broadcastToTask(task.jobKey, {
                type: 'status',
                jobKey: task.jobKey,
                progress: 100,
                status: 'failed',
                message: finalMessage
            });
        } finally {
            unregisterAdminGenerationStop(task.jobKey);
        }
    })();

    return { task, workerCount, targetCount };
}

function broadcastToTask(jobKey, data) {
    const clients = taskClients.get(jobKey);
    if (clients) {
        const message = JSON.stringify(data);
        for (const client of clients) {
            if (client.readyState === WebSocket.OPEN) {
                client.send(message);
            }
        }
    }
}

function unsubscribeTaskClient(jobKey, ws) {
    if (!jobKey || !taskClients.has(jobKey)) {
        return;
    }
    const clients = taskClients.get(jobKey);
    clients.delete(ws);
    if (clients.size === 0) {
        taskClients.delete(jobKey);
    }
}

async function sendTaskSnapshot(ws, jobKey) {
    const task = await store.getTaskStatus(jobKey);
    if (!task || ws.readyState !== WebSocket.OPEN) {
        return;
    }

    ws.send(JSON.stringify({
        type: 'snapshot',
        jobKey,
        status: task.status,
        message: task.message,
        progress: Number(task.progress || 0),
        cdkCode: task.cdk_code || null,
        phone: task.phone || null,
        cardLast4: task.card_last4 || null,
        isTerminal: TERMINAL_TASK_STATUSES.has(task.status)
    }));
}

function logTask(jobKey, message, level = 'log') {
    runtimeLog.push({
        jobKey,
        level,
        source: 'task',
        text: String(message || '')
    });
    const logger = console[level] || console.log;
    logger(`[Task ${jobKey}] ${message}`);
}


let storeReadyPromise = null;

const JSON_BODY_LIMIT = process.env.JSON_BODY_LIMIT || '15mb';
app.use(express.json({ limit: JSON_BODY_LIMIT }));
app.use(express.static(path.join(__dirname, 'public')));

function ensureStoreReady() {
    if (!storeReadyPromise) {
        storeReadyPromise = store.ensureReady().catch((error) => {
            storeReadyPromise = null;
            throw error;
        });
    }
    return storeReadyPromise;
}


function encodeBase64Url(input) {
    return Buffer.from(input)
        .toString('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/g, '');
}

function decodeBase64Url(input) {
    const normalized = input
        .replace(/-/g, '+')
        .replace(/_/g, '/')
        .padEnd(Math.ceil(input.length / 4) * 4, '=');
    return Buffer.from(normalized, 'base64').toString('utf8');
}

function signTokenPayload(encodedPayload) {
    return crypto
        .createHmac('sha256', ADMIN_TOKEN_SECRET)
        .update(encodedPayload)
        .digest('base64')
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=+$/g, '');
}

function safeEqualString(left, right) {
    const leftBuffer = Buffer.from(String(left));
    const rightBuffer = Buffer.from(String(right));
    if (leftBuffer.length !== rightBuffer.length) {
        return false;
    }
    return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function createPasswordHash(password, salt) {
    return crypto.scryptSync(String(password), salt, 64).toString('hex');
}

function verifyPassword(password, storedHash) {
    const parts = String(storedHash || '').split('$');
    if (parts.length !== 3 || parts[0] !== 'scrypt') {
        return false;
    }

    const [, salt, expectedHash] = parts;
    const actualHash = createPasswordHash(password, salt);
    return safeEqualString(actualHash, expectedHash);
}

function issueAdminToken(passwordVersion) {
    const now = Date.now();
    const payload = {
        sub: 'admin',
        permissions: ['admin'],
        pv: Math.max(1, Number(passwordVersion || 1)),
        iat: now,
        exp: now + ADMIN_TOKEN_TTL_MS
    };
    const encodedPayload = encodeBase64Url(JSON.stringify(payload));
    const signature = signTokenPayload(encodedPayload);
    return {
        token: `${encodedPayload}.${signature}`,
        payload
    };
}

function verifyAdminToken(token) {
    if (!token || typeof token !== 'string') {
        return null;
    }

    const [encodedPayload, signature] = token.split('.');
    if (!encodedPayload || !signature) {
        return null;
    }

    const expectedSignature = signTokenPayload(encodedPayload);
    if (!safeEqualString(signature, expectedSignature)) {
        return null;
    }

    try {
        const payload = JSON.parse(decodeBase64Url(encodedPayload));
        if (!payload || payload.sub !== 'admin' || !Array.isArray(payload.permissions)) {
            return null;
        }
        if (!payload.permissions.includes('admin')) {
            return null;
        }
        if (!payload.exp || Date.now() >= Number(payload.exp)) {
            return null;
        }
        return payload;
    } catch (error) {
        return null;
    }
}

function getBearerToken(req) {
    const header = req.headers.authorization || '';
    const [scheme, token] = header.split(' ');
    if (scheme === 'Bearer' && token) {
        return token.trim();
    }
    return (req.query?.token || '').trim() || null;
}

async function authenticateAdmin(req, res, next) {
    const token = getBearerToken(req);
    const payload = verifyAdminToken(token);
    if (!payload) {
        return res.status(401).json({ success: false, message: '未授权，请重新登录' });
    }

    try {
        await ensureStoreReady();
        const authConfig = await store.getAdminAuthConfig();
        if (Number(payload.pv || 0) !== authConfig.passwordVersion) {
            return res.status(401).json({ success: false, message: '登录状态已失效，请重新登录' });
        }
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }

    req.admin = payload;
    req.adminToken = token;
    return next();
}

function createCdks(count) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    const results = new Set();
    const target = Math.max(1, Math.min(Number(count) || 1, 100));

    while (results.size < target) {
        let cdk = '';
        for (let i = 0; i < 12; i += 1) {
            cdk += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        results.add(cdk);
    }

    return [...results];
}


app.get('/admin', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

app.get(['/admin-login', '/admin-login/', '/admin-login.html'], (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'admin-login.html'));
});

app.post('/api/admin/login', async (req, res) => {
    const password = String(req.body?.password || '');

    try {
        await ensureStoreReady();
        const authConfig = await store.getAdminAuthConfig();
        if (!verifyPassword(password, authConfig.passwordHash)) {
            return res.status(401).json({ success: false, message: '密码错误' });
        }

        const { token, payload } = issueAdminToken(authConfig.passwordVersion);
        return res.json({
            success: true,
            token,
            expiresAt: payload.exp,
            issuedAt: payload.iat,
            permissions: payload.permissions
        });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

/** 运行日志：必须挂在 app.use('/api/admin', authenticateAdmin) 之前，并为每条路由单独鉴权，否则部分环境下会 404 */
app.get('/api/admin/runtime-logs', authenticateAdmin, (req, res) => {
    try {
        const wantTail = String(req.query.tail || '') === '1' || String(req.query.tail || '') === 'true';
        const after = Math.max(0, parseInt(String(req.query.after || '0'), 10) || 0);
        let limit = parseInt(String(req.query.limit || '500'), 10) || 500;
        limit = Math.min(2000, Math.max(1, limit));

        const entries = wantTail ? runtimeLog.tail(limit) : runtimeLog.after(after, limit);
        const nextAfter = entries.length ? entries[entries.length - 1].id : after;
        res.json({ success: true, entries, nextAfter });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/admin/runtime-logs/clear', authenticateAdmin, (req, res) => {
    try {
        runtimeLog.clear();
        runtimeLog.push({ jobKey: '', level: 'system', source: 'server', text: '🧹 运行日志已手动清空' });
        res.json({ success: true, message: '运行日志已清空' });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

/** 与 runtime-logs 相同：必须挂在 app.use('/api/admin', authenticateAdmin) 之前并单独鉴权，否则部分环境下该 POST 会 404 */
app.post('/api/admin/products/generate-stop', authenticateAdmin, async (req, res) => {
    try {
        const jobKey = String(req.body?.jobKey || '').trim();
        if (jobKey) {
            const ok = requestAdminGenerationStop(jobKey);
            return res.json({
                success: true,
                stopped: ok ? 1 : 0,
                message: ok
                    ? '已发送停止指令：本批次不再开始新的成品条次（当前条次会跑完当前步骤后退出）'
                    : '未找到该 Job 的运行中批次，可能已结束或未在本进程启动'
            });
        }

        const keys = [...adminGenerationStopHandlers.keys()];
        let stopped = 0;
        for (const key of keys) {
            if (requestAdminGenerationStop(key)) {
                stopped += 1;
            }
        }

        return res.json({
            success: true,
            stopped,
            message: stopped > 0
                ? `已向 ${stopped} 个后台批次发送停止指令`
                : '当前没有在本进程内登记的后台成品批量任务（若任务刚结束请在任务管理中刷新列表）'
        });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

/** 代理批量测试：和 pool-emails 一样必须挂在 app.use 之前。
 *  支持 http(s) / socks5 / socks (走 proxy-agent 自动识别协议)。 */
app.post('/api/admin/proxy/test', authenticateAdmin, async (req, res) => {
    try {
        const { ProxyAgent } = require('proxy-agent');
        const proxies = Array.isArray(req.body?.proxies) ? req.body.proxies : [];
        const cleaned = proxies.map((p) => String(p || '').trim()).filter(Boolean);
        if (!cleaned.length) {
            return res.status(400).json({ success: false, message: '未提供代理 URL' });
        }
        if (cleaned.length > 50) {
            return res.status(400).json({ success: false, message: '一次最多测试 50 条代理' });
        }

        const PROBE_URLS = [
            'https://api.ipify.org/?format=text',
            'https://ifconfig.me/ip'
        ];

        const subst = (raw) => {
            if (!/\{session\}/i.test(raw)) return raw;
            const sid = `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 8)}`;
            return raw.replace(/\{session\}/gi, sid);
        };

        const testOne = async (raw) => {
            const proxyUrl = subst(raw);
            const t0 = Date.now();
            let agent;
            try {
                agent = new ProxyAgent({ getProxyForUrl: () => proxyUrl });
            } catch (e) {
                return { ok: false, error: `代理 URL 解析失败: ${e.message}`, latencyMs: Date.now() - t0 };
            }
            let lastErr = '';
            for (const probeUrl of PROBE_URLS) {
                try {
                    const r = await axios.get(probeUrl, {
                        httpsAgent: agent,
                        httpAgent: agent,
                        proxy: false,
                        timeout: 12000,
                        validateStatus: () => true
                    });
                    if (r.status === 200) {
                        const ip = String(r.data || '').trim().split(/\s+/)[0];
                        return { ok: true, ip, latencyMs: Date.now() - t0, probedVia: probeUrl };
                    }
                    lastErr = `HTTP ${r.status} via ${probeUrl}`;
                } catch (e) {
                    lastErr = `${e.code || ''} ${e.message}`.trim();
                }
            }
            return { ok: false, error: lastErr || '未知错误', latencyMs: Date.now() - t0 };
        };

        const results = await Promise.all(cleaned.map((p) => testOne(p)));
        return res.json({ success: true, results });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

/** 邮箱池：与 runtime-logs 相同，必须挂在 app.use('/api/admin', authenticateAdmin) 之前并单独鉴权，否则部分环境下会 404 */
app.get('/api/admin/pool-emails', authenticateAdmin, async (req, res) => {
    try {
        await ensureStoreReady();
        const items = await store.listPoolEmails();
        res.json({ success: true, items });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/admin/pool-emails/import', authenticateAdmin, async (req, res) => {
    try {
        await ensureStoreReady();
        const text = String(req.body?.text ?? '');
        const result = await store.bulkImportPoolEmails(text);
        const parts = [`已写入或合并 ${result.applied} 条邮箱记录`];
        if (result.oauthCount) {
            parts.push(`其中 OAuth2 ${result.oauthCount} 条`);
        }
        if (result.skipped) {
            parts.push(`跳过非法行 ${result.skipped} 行`);
        }
        res.json({
            success: true,
            message: parts.join('，'),
            applied: result.applied,
            parsed: result.parsed,
            oauthCount: result.oauthCount,
            skipped: result.skipped
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.delete('/api/admin/pool-emails/:id', authenticateAdmin, async (req, res) => {
    try {
        await ensureStoreReady();
        await store.deletePoolEmail(Number(req.params.id));
        res.json({ success: true, message: '邮箱记录已删除' });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/admin/pool-emails/:id/messages', authenticateAdmin, async (req, res) => {
    try {
        await ensureStoreReady();
        const row = await store.getPoolEmailCredentials(Number(req.params.id));
        if (!row) {
            return res.status(400).json({ success: false, message: '邮箱不存在' });
        }
        if (!row.refreshToken && !row.password) {
            return res.status(400).json({ success: false, message: '邮箱未配置可用凭证 (OAuth2 / 密码)' });
        }
        const host = await store.getAppConfigValue('pool_email_imap_host', 'outlook.office365.com');
        const includeJunk = String(await store.getAppConfigValue('pool_email_include_junk', '1')) === '1';
        const messages = await listRecentEmailsForAdmin({
            email: row.email,
            password: row.password,
            clientId: row.clientId,
            refreshToken: row.refreshToken,
            host: String(host || 'outlook.office365.com').trim() || 'outlook.office365.com',
            includeJunk,
            limit: Math.min(80, Math.max(5, Number(req.query.limit) || 40))
        });
        res.json({ success: true, messages });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.use('/api/admin', authenticateAdmin);

app.get('/api/public/runtime', async (req, res) => {
    try {
        await ensureStoreReady();
        const maxConcurrentActivations = await store.getMaxConcurrentActivations();
        return res.json({
            success: true,
            runtime: {
                active_foreground_jobs: activeForegroundJobs.size,
                max_foreground_jobs: Math.max(1, Number(maxConcurrentActivations || 1))
            }
        });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/admin/session', (req, res) => {
    const age = Date.now() - Number(req.admin.iat || 0);
    const shouldRefresh = age >= ADMIN_REFRESH_AFTER_MS;
    let refreshedToken = null;
    let payload = req.admin;

    if (shouldRefresh) {
        const refreshed = issueAdminToken(req.admin.pv);
        refreshedToken = refreshed.token;
        payload = refreshed.payload;
    }

    return res.json({
        success: true,
        refreshed: shouldRefresh,
        token: refreshedToken,
        expiresAt: payload.exp,
        issuedAt: payload.iat,
        permissions: payload.permissions
    });
});

app.get('/api/admin/data', async (req, res) => {
    try {
        await ensureStoreReady();
        await syncAccessDeactivatedProductStatuses();
        const data = await store.getAdminData();
        const system = await getSystemMetrics();
        data.runtime = {
            active_activation_jobs: getTotalActiveJobs(),
            active_background_jobs: activeBackgroundJobs.size,
            active_foreground_jobs: activeForegroundJobs.size,
            system
        };
        res.json(data);
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.delete('/api/admin/task-logs/:jobKey', async (req, res) => {
    try {
        await ensureStoreReady();
        const jobKey = decodeURIComponent(String(req.params.jobKey || '').trim());
        if (!jobKey) {
            return res.status(400).json({ success: false, message: '缺少任务标识' });
        }
        const { deleted } = await store.deleteTaskLogByJobKey(jobKey);
        if (!deleted) {
            return res.status(404).json({ success: false, message: '未找到该任务记录' });
        }
        return res.json({ success: true, message: '任务记录已删除' });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/admin/config', async (req, res) => {
    try {
        await ensureStoreReady();
        const nextConfig = { ...(req.body || {}) };
        if (nextConfig.maintenance_mode) {
            nextConfig.maintenance_mode_drain = getTotalActiveJobs() > 0;
        } else {
            nextConfig.maintenance_mode_drain = false;
        }
        await store.saveConfig(nextConfig);
        res.json({ success: true, message: '所有资产配置已保存' });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/admin/change-password', async (req, res) => {
    const currentPassword = String(req.body?.currentPassword || '');
    const newPassword = String(req.body?.newPassword || '').trim();

    if (!currentPassword) {
        return res.status(400).json({ success: false, message: '请输入原密码' });
    }

    if (newPassword.length < 6) {
        return res.status(400).json({ success: false, message: '新密码至少 6 位' });
    }

    try {
        await ensureStoreReady();
        const authConfig = await store.getAdminAuthConfig();

        if (!verifyPassword(currentPassword, authConfig.passwordHash)) {
            return res.status(400).json({ success: false, message: '原密码错误' });
        }

        if (verifyPassword(newPassword, authConfig.passwordHash)) {
            return res.status(400).json({ success: false, message: '新密码不能与原密码相同' });
        }

        await store.updateAdminPassword(newPassword);
        return res.json({ success: true, message: '密码修改成功，请重新登录' });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/admin/cdks', async (req, res) => {
    try {
        await ensureStoreReady();
        res.json(await store.listCdks());
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/admin/cdks/generate', async (req, res) => {
    try {
        await ensureStoreReady();
        const count = req.body?.count;
        const type = '成品';
        const newCdks = createCdks(count);
        const result = await store.insertCdks(newCdks, { type });
        res.json({
            success: true,
            message: `成功生成 ${newCdks.length} 个 ${type} CDK (数据库写入: ${result.insertedCount})`,
            cdks: newCdks,
            insertedCount: result.insertedCount
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/admin/cdks/import', async (req, res) => {
    const cdks = Array.isArray(req.body?.cdks) ? req.body.cdks : [];
    if (cdks.length === 0) {
        return res.status(400).json({ success: false, message: '请提供要导入的卡密' });
    }

    try {
        await ensureStoreReady();
        const summary = await store.insertCdks(cdks);
        res.json({
            success: true,
            message: `导入完成，新增 ${summary.insertedCount} 个，重复 ${summary.duplicateCount} 个`,
            ...summary
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/admin/cdks/:cdk/ship', async (req, res) => {
    try {
        await ensureStoreReady();
        const updated = await store.markCdkShipped(req.params.cdk);
        if (!updated) {
            return res.status(404).json({ success: false, message: 'CDK 不存在' });
        }
        res.json({ success: true, message: 'CDK 已标记出库' });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.delete('/api/admin/cdks/:cdk', async (req, res) => {
    try {
        await ensureStoreReady();
        await store.deleteCdk(req.params.cdk);
        res.json({ success: true, message: 'CDK 已删除' });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// 成品号管理
app.get('/api/admin/products', async (req, res) => {
    try {
        await ensureStoreReady();
        await syncAccessDeactivatedProductStatuses();
        res.json(await store.listProducts());
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.delete('/api/admin/products/:id', async (req, res) => {
    try {
        await ensureStoreReady();
        await store.deleteProduct(req.params.id);
        res.json({ success: true, message: '成品号已删除' });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.put('/api/admin/products/:id/status', async (req, res) => {
    try {
        await ensureStoreReady();
        const { status } = req.body;
        if (!['正常', '封禁'].includes(status)) {
            return res.status(400).json({ success: false, message: '无效的状态' });
        }
        await store.updateProductStatus(req.params.id, status);
        res.json({ success: true, message: '状态已更新', status });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

function sanitizeExportFileName(name) {
    return String(name || '')
        .replace(/[\\/:*?"<>|]/g, '_')
        .replace(/\s+/g, ' ')
        .trim();
}

function buildCpaProductFileName(email, planType, accountId) {
    const safePlanType = String(planType || 'plus').trim() || 'plus';
    const safeEmail = String(email || '').trim();
    const hashAccountId = accountId
        ? crypto.createHash('sha256').update(String(accountId)).digest('hex').slice(0, 8)
        : '';

    return hashAccountId
        ? `${safeEmail}-${safePlanType}-${hashAccountId}.json`
        : `${safeEmail}-${safePlanType}.json`;
}

function resolveArtifactAbsolutePath(filePathValue) {
    if (!filePathValue) {
        return '';
    }
    return path.isAbsolute(filePathValue)
        ? filePathValue
        : path.join(__dirname, filePathValue);
}

function resolveNamedArtifactPath(kind, filename) {
    const normalizedFileName = String(filename || '').trim();
    if (!normalizedFileName) {
        return '';
    }

    const preferredDir = path.join(__dirname, 'product_files', kind);
    const preferredPath = path.join(preferredDir, normalizedFileName);
    if (fs.existsSync(preferredPath)) {
        return preferredPath;
    }

    const legacyRootPath = path.join(__dirname, 'product_files', normalizedFileName);
    if (fs.existsSync(legacyRootPath)) {
        return legacyRootPath;
    }

    return preferredPath;
}

function resolveProductArtifactInfo(downloadInfo) {
    const sub2apiPath = resolveArtifactAbsolutePath(downloadInfo?.filePath || '');
    const artifactInfo = {
        email: String(downloadInfo?.email || '').trim(),
        sub2apiPath: '',
        sub2apiFileName: null,
        cpaPath: '',
        cpaFileName: null
    };

    if (!sub2apiPath || !fs.existsSync(sub2apiPath)) {
        return artifactInfo;
    }

    artifactInfo.sub2apiPath = sub2apiPath;
    artifactInfo.sub2apiFileName = path.basename(sub2apiPath);

    try {
        const payload = JSON.parse(fs.readFileSync(sub2apiPath, 'utf8'));
        const entry = Array.isArray(payload?.accounts) ? payload.accounts[0] : null;
        const email = artifactInfo.email || String(entry?.name || entry?.extra?.email || '').trim();
        const planType = String(entry?.plan_type || 'plus').trim() || 'plus';
        const accountId = String(entry?.credentials?.chatgpt_account_id || '').trim();
        const cpaFileName = email ? `${email}.json` : '';
        let cpaPath = cpaFileName ? path.join(__dirname, 'product_files', 'cpa', cpaFileName) : '';

        if (cpaPath && !fs.existsSync(cpaPath) && email) {
            const legacyCpaFileName = buildCpaProductFileName(email, planType, accountId);
            cpaPath = path.join(__dirname, 'product_files', legacyCpaFileName);
        }

        artifactInfo.email = email;
        if (cpaPath && fs.existsSync(cpaPath)) {
            artifactInfo.cpaPath = cpaPath;
            artifactInfo.cpaFileName = path.basename(cpaPath);
        }
    } catch (_) { }

    return artifactInfo;
}

function sendJsonFileDownload(res, fullPath) {
    const fileName = path.basename(fullPath);
    res.setHeader('Content-Disposition', `attachment; filename=${fileName}`);
    res.setHeader('Content-Type', 'application/json');
    fs.createReadStream(fullPath).pipe(res);
}

async function buildProductExportFile(products, exportPrefix, options = {}) {
    const mergedAccounts = [];

    for (const product of products) {
        if (String(product.plan_type || 'plus').toLowerCase() === 'free') {
            mergedAccounts.push({
                name: product.email,
                email: product.email,
                access_token: product.token || '',
                plan_type: 'free'
            });
            continue;
        }

        if (!product.file_path) {
            throw new Error(`成品号 ${product.email} 缺少配置文件`);
        }

        const fullPath = path.isAbsolute(product.file_path)
            ? product.file_path
            : path.join(__dirname, product.file_path);

        if (!fs.existsSync(fullPath)) {
            throw new Error(`成品号 ${product.email} 的配置文件不存在`);
        }

        const raw = fs.readFileSync(fullPath, 'utf8');
        const payload = JSON.parse(raw);
        const accounts = Array.isArray(payload?.accounts) ? payload.accounts : [];
        mergedAccounts.push(...accounts);
    }

    const explicitFileName = String(options.fileName || '').trim();
    const fileName = explicitFileName
        ? sanitizeExportFileName(explicitFileName)
        : `${sanitizeExportFileName(exportPrefix)}_${Date.now()}.json`;

    return {
        fileName,
        fileBuffer: Buffer.from(JSON.stringify({
            exported_at: new Date().toISOString(),
            proxies: [],
            accounts: mergedAccounts
        }, null, 2), 'utf8')
    };
}

app.post('/api/admin/products/export', async (req, res) => {
    try {
        await ensureStoreReady();
        const { ids } = req.body || {};
        if (!Array.isArray(ids) || ids.length === 0) {
            return res.status(400).json({ success: false, message: '请先选择要出库的成品号' });
        }

        const products = await store.listProducts();
        const selectedProducts = ids
            .map((id) => products.find((item) => String(item.id) === String(id)))
            .filter(Boolean);

        if (selectedProducts.length !== ids.length) {
            return res.status(404).json({ success: false, message: '部分成品号不存在或已被删除' });
        }

        const { fileName, fileBuffer } = await buildProductExportFile(selectedProducts, '成品号批量出库');

        for (const product of selectedProducts) {
            await store.markProductShipped(product.id);
        }

        res.setHeader('Content-Disposition', `attachment; filename=${encodeURIComponent(fileName)}`);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.send(fileBuffer);
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/admin/products/:id/export', async (req, res) => {
    try {
        await ensureStoreReady();
        const targetId = String(req.params.id || '').trim();
        if (!targetId) {
            return res.status(400).send('Missing product id');
        }

        const products = await store.listProducts();
        const product = products.find((item) => String(item.id) === targetId);
        if (!product) {
            return res.status(404).send('Product not found');
        }

        const { fileName, fileBuffer } = await buildProductExportFile(
            [product],
            `成品号出库_${product.email}`,
            { fileName: `${product.email}.json` }
        );
        await store.markProductShipped(product.id);

        res.setHeader('Content-Disposition', `attachment; filename=${encodeURIComponent(fileName)}`);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.send(fileBuffer);
    } catch (error) {
        res.status(500).send(error.message);
    }
});

const { startProductCreation } = require('./product_activator');

app.post('/api/admin/products/generate', async (req, res) => {
    const count = Math.max(1, Math.min(Number(req.body?.count) || 1, 100));
    const planType = String(req.body?.planType || 'plus').trim().toLowerCase();
    if (!['free', 'plus'].includes(planType)) {
        return res.status(400).json({ success: false, message: '流程类型必须是 free 或 plus' });
    }

    try {
        await ensureStoreReady();
        const maintenanceModeState = await store.getMaintenanceModeState();
        if (maintenanceModeState.enabled) {
            return res.status(503).json({ success: false, message: '系统维护中，请稍后再试' });
        }
        const launched = await startAdminProductGenerationTask(count, { planType });

        return res.json({
            success: true,
            jobKey: launched.task.jobKey,
            workerCount: launched.workerCount,
            message: `${planType === 'free' ? 'Free 注册' : 'Plus 成品'}任务已启动，并发上限 ${launched.workerCount}`
        });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/admin/products/resume', async (req, res) => {
    try {
        await ensureStoreReady();
        const maintenanceModeState = await store.getMaintenanceModeState();
        if (maintenanceModeState.enabled) {
            return res.status(503).json({ success: false, message: '系统维护中，请稍后再试' });
        }

        const resumableTask = await store.getResumableAdminProductGeneration();
        if (!resumableTask || resumableTask.remainingCount <= 0) {
            return res.status(400).json({ success: false, message: '当前没有可继续生产的中断任务' });
        }

        const launched = await startAdminProductGenerationTask(resumableTask.remainingCount, {
            resumeFrom: resumableTask,
            planType: resumableTask.planType || 'plus'
        });

        return res.json({
            success: true,
            jobKey: launched.task.jobKey,
            workerCount: launched.workerCount,
            resumedCount: resumableTask.remainingCount,
            message: `已继续生产剩余 ${resumableTask.remainingCount} 个成品号`
        });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/redeem-product', async (req, res) => {
    const cdk = String(req.body?.cdk || '').trim();
    if (!cdk) {
        return res.status(400).json({ success: false, message: '缺少 CDK' });
    }

    try {
        await ensureStoreReady();
        const maintenanceModeState = await store.getMaintenanceModeState();
        if (maintenanceModeState.enabled) {
            return res.status(503).json({ success: false, message: '系统维护中，请稍后再试' });
        }

        // 前台兑换只从现有号池原子出库，不创建注册、支付或激活任务。
        const claimed = await store.claimProductAccount(cdk);
        const downloadInfo = await store.getClaimedProductDownloadInfo(cdk);
        const artifactInfo = resolveProductArtifactInfo(downloadInfo);

        return res.json({
            success: true,
            completed: true,
            message: '成品账号领取成功',
            result: {
                email: claimed.email,
                imapKey: claimed.imapKey || '',
                planType: claimed.planType || 'plus',
                downloadAvailable: Boolean(artifactInfo.sub2apiPath || downloadInfo?.planType === 'free'),
                sub2apiAvailable: Boolean(artifactInfo.sub2apiPath || downloadInfo?.planType === 'free'),
                cpaAvailable: Boolean(artifactInfo.cpaPath)
            }
        });
    } catch (error) {
        const message = String(error?.message || error);
        const status = message.includes('CDK 无效') ? 403
            : (message.includes('缺货') ? 409 : 500);
        return res.status(status).json({ success: false, message });
    }
});

app.get('/api/download-sub2api/:filename', async (req, res) => {
    const filename = req.params.filename;
    if (!filename || !/^[a-zA-Z0-9.@_-]+\.json$/.test(filename)) {
        return res.status(400).send('Invalid filename');
    }

    const filePath = resolveNamedArtifactPath('sub2api', filename);
    if (!fs.existsSync(filePath)) {
        return res.status(404).send('File not found');
    }

    sendJsonFileDownload(res, filePath);
});

app.get('/api/download-cpa/:filename', async (req, res) => {
    const filename = req.params.filename;
    if (!filename || !/^[a-zA-Z0-9.@_-]+\.json$/.test(filename)) {
        return res.status(400).send('Invalid filename');
    }

    const filePath = resolveNamedArtifactPath('cpa', filename);
    if (!fs.existsSync(filePath)) {
        return res.status(404).send('File not found');
    }

    sendJsonFileDownload(res, filePath);
});

app.post('/api/run-process', (req, res) => {
    return res.status(410).json({
        success: false,
        message: '该接口已下线，前台仅支持成品 CDK 兑换'
    });
});

app.post('/api/verify-cdk', async (req, res) => {
    const cdk = String(req.body?.cdk || '').trim();
    if (!cdk) {
        return res.status(400).json({ success: false, message: '请输入 CDK' });
    }

    try {
        await ensureStoreReady();
        const cdkData = await store.verifyCdkDetails(cdk);
        if (cdkData && cdkData.type !== '成品') {
            return res.status(403).json({ success: false, message: '该 CDK 不支持兑换' });
        }
        if (cdkData && !cdkData.used_at) {
            return res.json({
                success: true,
                data: {
                    type: '成品'
                }
            });
        }

        return res.status(403).json({ success: false, message: cdkData?.used_at ? '该 CDK 已使用' : '无效 CDK' });
    } catch (error) {
        return res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/cdk/query', async (req, res) => {
    const cdk = String(req.query.cdk || '').trim();
    if (!cdk) {
        return res.status(400).json({ success: false, message: '请输入查询激活码' });
    }

    try {
        await ensureStoreReady();
        const cdkData = await store.verifyCdkDetails(cdk);
        if (!cdkData) {
            return res.status(404).json({ success: false, message: '未找到该激活码记录' });
        }
        if (cdkData.type !== '成品') {
            return res.status(404).json({ success: false, message: '未找到该激活码记录' });
        }

        const cdkStatus = cdkData.used_at ? '已使用' : '未使用';

        const downloadInfo = cdkData.type === '成品' && cdkData.used_at
            ? await store.getClaimedProductDownloadInfo(cdk)
            : null;
        const artifactInfo = resolveProductArtifactInfo(downloadInfo);

        res.json({
            success: true,
            data: {
                status: cdkStatus,
                type: cdkData.type,
                createdAt: cdkData.created_at,
                usedAt: cdkData.used_at
                    ? new Date(cdkData.used_at).toLocaleString('zh-CN', { hour12: false }).replace(/\//g, '-')
                    : null,
                imapKey: downloadInfo?.imapKey || null,
                downloadAvailable: Boolean(artifactInfo.sub2apiPath || downloadInfo?.planType === 'free'),
                downloadFileName: artifactInfo.sub2apiFileName || (downloadInfo?.planType === 'free' ? `${downloadInfo.email}-free.json` : null),
                sub2apiAvailable: Boolean(artifactInfo.sub2apiPath || downloadInfo?.planType === 'free'),
                sub2apiFileName: artifactInfo.sub2apiFileName || (downloadInfo?.planType === 'free' ? `${downloadInfo.email}-free.json` : null),
                cpaAvailable: Boolean(artifactInfo.cpaPath),
                cpaFileName: artifactInfo.cpaFileName
            }
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/cdk/download', async (req, res) => {
    const cdk = String(req.query.cdk || '').trim();
    const kind = String(req.query.kind || 'sub2api').trim().toLowerCase();
    if (!cdk) {
        return res.status(400).send('Missing cdk');
    }
    if (!['sub2api', 'cpa'].includes(kind)) {
        return res.status(400).send('Invalid download kind');
    }

    try {
        await ensureStoreReady();
        const cdkData = await store.verifyCdkDetails(cdk);
        if (!cdkData || cdkData.type !== '成品' || !cdkData.used_at) {
            return res.status(403).send('CDK not eligible for download');
        }

        const downloadInfo = await store.getClaimedProductDownloadInfo(cdk);
        if (downloadInfo?.planType === 'free') {
            const fileName = `${sanitizeExportFileName(downloadInfo.email || cdk)}-free.json`;
            const fileBuffer = Buffer.from(JSON.stringify({
                exported_at: new Date().toISOString(),
                proxies: [],
                accounts: [{
                    name: downloadInfo.email,
                    email: downloadInfo.email,
                    access_token: downloadInfo.token || '',
                    plan_type: 'free'
                }]
            }, null, 2), 'utf8');
            res.setHeader('Content-Disposition', `attachment; filename=${encodeURIComponent(fileName)}`);
            res.setHeader('Content-Type', 'application/json; charset=utf-8');
            return res.send(fileBuffer);
        }
        if (!downloadInfo?.filePath) {
            return res.status(404).send('Credential file not found');
        }

        const artifactInfo = resolveProductArtifactInfo(downloadInfo);
        const fullPath = kind === 'cpa' ? artifactInfo.cpaPath : artifactInfo.sub2apiPath;

        if (!fullPath || !fs.existsSync(fullPath)) {
            return res.status(404).send('Credential file missing');
        }

        sendJsonFileDownload(res, fullPath);
    } catch (error) {
        res.status(500).send(error.message);
    }
});

async function start() {
    await ensureStoreReady();

    // 启动时把所有遗留的 in_use 锁清空（避免上次崩溃残留的锁卡死整个池）
    try {
        await store.resetAllAssetLocks();
        console.log('🔓 [资产锁] 启动时已重置所有 in_use 标记');
    } catch (error) {
        console.error(`❌ [资产锁] 启动重置失败: ${error.message}`);
    }

    // 每 60 秒兜底回收一次"超过 15 分钟仍未释放"的锁（防进程崩溃）
    setInterval(async () => {
        try {
            const released = await store.releaseStaleAssetLocks();
            if (released.phoneReleased > 0 || released.cardReleased > 0 || released.poolReleased > 0) {
                console.log(`🧹 [资产锁] 兜底回收  phone=${released.phoneReleased}  card=${released.cardReleased}  pool_emails=${released.poolReleased}`);
            }
        } catch (error) {
            console.warn(`⚠️  [资产锁] 周期清理失败: ${error.message}`);
        }
    }, 60 * 1000).unref();

    try {
        await initializeImapAuth();
        await syncAccessDeactivatedProductStatuses(true);
        scheduleAccessDeactivatedSync();
    } catch (error) {
        console.error(`[IMAP] 项目启动预刷新失败: ${error.message}`);
        scheduleAccessDeactivatedSync();
    }

    const server = app.listen(PORT, () => {
        const conn = store.connectionInfo;
        runtimeLog.push({
            jobKey: '',
            level: 'system',
            source: 'server',
            text: `✅ 服务就绪  http://localhost:${PORT}  ·  MySQL ${conn.user}@${conn.host}:${conn.port}/${conn.database}  ·  PID=${process.pid}`
        });
        console.log('数据库表检查完成');
        console.log(`http://localhost:${PORT}`);
        console.log(`MySQL => ${conn.user}@${conn.host}:${conn.port}/${conn.database}`);
    });

    // WebSocket Server Setup
    const wss = new WebSocket.Server({ server });
    wss.on('connection', (ws) => {
        let currentJobKey = null;

        ws.on('message', async (message) => {
            try {
                const data = JSON.parse(message);
                if (data.type === WS_HEARTBEAT_PING_TYPE) {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({
                            type: WS_HEARTBEAT_PONG_TYPE,
                            ts: Number(data.ts) || Date.now()
                        }));
                    }
                    return;
                }
                if (data.type === 'subscribe' && data.jobKey) {
                    if (currentJobKey && currentJobKey !== data.jobKey) {
                        unsubscribeTaskClient(currentJobKey, ws);
                    }
                    currentJobKey = data.jobKey;
                    if (!taskClients.has(currentJobKey)) {
                        taskClients.set(currentJobKey, new Set());
                    }
                    taskClients.get(currentJobKey).add(ws);
                    console.log(`Client subscribed to task: ${currentJobKey}`);
                    await sendTaskSnapshot(ws, currentJobKey);
                }
            } catch (e) {
                console.error('WebSocket message error:', e);
            }
        });

        ws.on('close', () => {
            unsubscribeTaskClient(currentJobKey, ws);
        });
    });
}

if (process.env.IS_PRODUCT_FLOW === 'true') {
    console.log('[系统] 检测到成品子流程环境，跳过 Web 服务监听。');
} else {
    start().catch((error) => {
        console.error('服务启动失败:', error.message);

        if (error && /ECONNREFUSED|connect/i.test(String(error.message || error))) {
            const conn = store.connectionInfo;
            console.error(
                `MySQL 连接配置 => ${conn.user}@${conn.host}:${conn.port}/${conn.database}`
            );
            console.error('排查建议:');
            console.error('1. 确认本机或远程 MySQL 已启动，并且监听了对应 host/port。');
            console.error(`2. 如果不是本机默认库，请先设置环境变量后再启动，例如:`);
            console.error(
                `   $env:DB_HOST='127.0.0.1'; $env:DB_PORT='3306'; $env:DB_USER='root'; $env:DB_PASSWORD='你的密码'; $env:DB_NAME='gpt'; node server.js`
            );
            console.error('3. 首次建库时，请先在 MySQL 中创建数据库，再启动服务自动建表。');
        }

        process.exit(1);
    });
}
