#!/bin/bash

[ -d handin ] || mkdir handin


cp -f  src/sql/execution/execute.rs handin/
cp -f  src/sql/execution/transform.rs handin/
cp -f src/sql/engine/local.rs handin/
cp -f  src/sql/execution/join.rs handin/
cp -f  src/sql/execution/write.rs handin/
cp -f  src/sql/execution/aggregate.rs handin/
cp -f src/storage/tuple/row.rs handin/
cp -f src/storage/page/table_page/table_page.rs  handin/
cp -f  src/storage/buffer/buffer_pool_manager/buffer_pool_manager.rs handin/
cp -f  src/storage/buffer/lru_k_replacer/lru_k_replacer.rs handin/
cp -f src/sql/execution/source.rs handin/
