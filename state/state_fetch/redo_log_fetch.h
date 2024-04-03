//
// Created by xuyanshi on 1/27/24.
//
// TODO: 本文件还没加入cmake，应该在合适位置导入
//

#pragma once

#include "state/state_store/redo_log.h"
#include "state/state_store/txn_list.h"

// #include "sql_class.h"

// #include "sql/sql_thd_internal_api.h"

// @StateReplicate
#include "sql/handler.h"
#include "state/util/txn_list_util.h"

#include <iostream>
#include <vector>

// 包括了QP，MetaManager等
#include "storage/innobase/include/log0sys.h"

/**
 * @StateReplicate: 定义了 redo log 的状态取回
 */
class RedoLogFetch {
 public:
  RedoLogFetch() = default;

  ~RedoLogFetch() = default;

  RedoLogFetch(bool status) : failStatus(status) {}

  void setFailStatus(bool status) { failStatus = status; }

  bool getFailStatus() const { return failStatus; }

  void setRedoLogItem(RedoLogItem *item) { redoLogItem = item; }

  RedoLogItem *getRedoLogItem() const { return redoLogItem; }

  void setRedoLogBufferBuf(unsigned char *buffer) { log_buf_data = buffer; }
  unsigned char *getRedoLogBufferBuf() const { return log_buf_data; }

  /**
   * @StateReplicate: 把 redo log buffer 从状态层读回来
   * @return
   */
  bool redo_log_fetch(log_t &log) {
    // this->log = log;
    primary_node_id = MetaManager::get_instance()->GetPrimaryNodeID();
    qp = log.qp_manager->GetRemoteLogBufQPWithNodeID(primary_node_id);
    meta_mgr = MetaManager::get_instance();

    // 取回 redo log buffer 的元数据
    size_t redo_log_buf_size = sizeof(RedoLogItem);
    redoLogItem =
        (RedoLogItem *)log.rdma_buffer_allocator->Alloc(redo_log_buf_size);
    if (!log.coro_sched->RDMAReadSync(0, qp, (char *)redoLogItem,
                                      meta_mgr->GetRedoLogCurrAddr(),
                                      redo_log_buf_size)) {
      // Fail
      std::cout << "failed to read redo_log_remote_buf\n";
      assert(0);
      return false;
    }

    // 取回 redo log buffer 的实际数据
    // TODO:这里的size还不确定，需要与storage/innobase/log/log0buf.cc:1133保持一致
    size_t log_buf_data_size = ut::INNODB_CACHE_LINE_SIZE;  // log.buf_size;
    log_buf_data = (byte *)log.rdma_buffer_allocator->Alloc(log_buf_data_size);
    if (!log.coro_sched->RDMAReadSync(
            0, qp, (char *)log_buf_data,
            meta_mgr->GetRedoLogCurrAddr() + sizeof(RedoLogItem),
            log_buf_data_size)) {
      // Fail
      std::cout << "failed to read log_buf_data\n";
      assert(0);
      return false;
    }

    return true;
  }

  /**
   * Created by xuyanshi on 4/2/24.
   *
   * @StateReplicate: 把 log 和 ATT 从状态节点读回来并解析 log ，然后根据
   * sql_lsn 依次判断哪些需要遍历，哪些不需要
   *
   * @param log
   * @return
   */
  std::vector<bool> redo_log_parse_and_judge(log_t &log, THD *thd) {
    // 把 log 和 ATT 从状态节点读回来
    redo_log_fetch(log);
    txn_items = GetTxnItemsFromRemote(thd);

    std::vector<bool> valid_log;  // (txn_items.size(), false);
    for (int i = 0; i < txn_items.size(); ++i) {
      TxnItem *cur_active_txn = txn_items[i];
      lsn_t sql_lsn = cur_active_txn->sql_lsn;  // 朝阳还没写完，先在这里调用
      // 遍历日志，解析 log 看属于哪个事务
    }
    return valid_log;
  }

  /**
   * @StateReplicate: TODO: 回放 buffer 中存储的 log，实现状态恢复
   * @return
   */
  bool redo_log_replay() { return true; }

 private:
  // failStatus 为真，则说明需要进行故障恢复，继续之后的逻辑
  bool failStatus = false;

  // redo log buffer 的元信息，即原来的 log
  RedoLogItem *redoLogItem = nullptr;

  // redo log buffer 的实际数据，即原来的 log.buf
  unsigned char *log_buf_data = nullptr;

  // 从状态层里面读到的活跃事务列表，里面有需要用到的 SQL-LSN
  std::vector<TxnItem *> txn_items;

  node_id_t primary_node_id =
      0;  // MetaManager::get_instance()->GetPrimaryNodeID();
  RCQP *qp =
      nullptr;  // thd->qp_manager->GetRemoteLogBufQPWithNodeID(primary_node_id);
  MetaManager *meta_mgr = nullptr;  // MetaManager::get_instance();
};