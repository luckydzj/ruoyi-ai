-- ============================================================
-- 内容管理（CMS）菜单权限脚本
-- 2026-08-15
-- 菜单 ID 使用 2099010100000000040–2099010100000000045 段（已验证空区间），
-- 与现有 snowflake id（2xxxxxxxxxxxxxxxxx）及 2099010100000000xxx 既有段不冲突。
-- 幂等：每行通过 WHERE NOT EXISTS 同时检查固定 menu_id 与对应的 path/component/perms 语义，
-- 避免同语义菜单已用其他 ID 存在时重复插入。
-- ============================================================

-- 目录：内容管理 (parent_id=0, path='cms', menu_type='M')
INSERT INTO `sys_menu` (`menu_id`, `menu_name`, `parent_id`, `order_num`, `path`, `component`, `query_param`, `is_frame`, `is_cache`, `menu_type`, `visible`, `status`, `perms`, `icon`, `create_dept`, `create_by`, `create_time`, `update_by`, `update_time`, `remark`)
SELECT 2099010100000000040, '内容管理', 0, 6, 'cms', '', '', 1, 0, 'M', '0', '0', '', 'fluent:content-24-regular', 103, 1, '2026-08-15 00:00:00', NULL, NULL, '内容管理目录'
WHERE NOT EXISTS (
    SELECT 1 FROM `sys_menu` WHERE `menu_id` = 2099010100000000040
    UNION
    SELECT 1 FROM `sys_menu` WHERE `path` = 'cms' AND `menu_type` = 'M' AND `parent_id` = 0
);

-- 页面：内容列表 (parent_id=2099010100000000040, component='cms/content/index', menu_type='C')
INSERT INTO `sys_menu` (`menu_id`, `menu_name`, `parent_id`, `order_num`, `path`, `component`, `query_param`, `is_frame`, `is_cache`, `menu_type`, `visible`, `status`, `perms`, `icon`, `create_dept`, `create_by`, `create_time`, `update_by`, `update_time`, `remark`)
SELECT 2099010100000000041, '内容列表', 2099010100000000040, 1, 'content', 'cms/content/index', NULL, 1, 0, 'C', '0', '0', 'cms:content:list', 'fluent:document-text-24-regular', 103, 1, '2026-08-15 00:00:00', NULL, NULL, '内容列表菜单'
WHERE NOT EXISTS (
    SELECT 1 FROM `sys_menu` WHERE `menu_id` = 2099010100000000041
    UNION
    SELECT 1 FROM `sys_menu` WHERE `component` = 'cms/content/index' AND `menu_type` = 'C'
);

-- 按钮：内容查询 (perms='cms:content:query')
INSERT INTO `sys_menu` (`menu_id`, `menu_name`, `parent_id`, `order_num`, `path`, `component`, `query_param`, `is_frame`, `is_cache`, `menu_type`, `visible`, `status`, `perms`, `icon`, `create_dept`, `create_by`, `create_time`, `update_by`, `update_time`, `remark`)
SELECT 2099010100000000042, '内容查询', 2099010100000000041, 1, '#', '', NULL, 1, 0, 'F', '0', '0', 'cms:content:query', '#', 103, 1, '2026-08-15 00:00:00', NULL, NULL, ''
WHERE NOT EXISTS (
    SELECT 1 FROM `sys_menu` WHERE `menu_id` = 2099010100000000042
    UNION
    SELECT 1 FROM `sys_menu` WHERE `perms` = 'cms:content:query' AND `menu_type` = 'F'
);

-- 按钮：内容新增 (perms='cms:content:add')
INSERT INTO `sys_menu` (`menu_id`, `menu_name`, `parent_id`, `order_num`, `path`, `component`, `query_param`, `is_frame`, `is_cache`, `menu_type`, `visible`, `status`, `perms`, `icon`, `create_dept`, `create_by`, `create_time`, `update_by`, `update_time`, `remark`)
SELECT 2099010100000000043, '内容新增', 2099010100000000041, 2, '#', '', NULL, 1, 0, 'F', '0', '0', 'cms:content:add', '#', 103, 1, '2026-08-15 00:00:00', NULL, NULL, ''
WHERE NOT EXISTS (
    SELECT 1 FROM `sys_menu` WHERE `menu_id` = 2099010100000000043
    UNION
    SELECT 1 FROM `sys_menu` WHERE `perms` = 'cms:content:add' AND `menu_type` = 'F'
);

-- 按钮：内容修改 (perms='cms:content:edit')
INSERT INTO `sys_menu` (`menu_id`, `menu_name`, `parent_id`, `order_num`, `path`, `component`, `query_param`, `is_frame`, `is_cache`, `menu_type`, `visible`, `status`, `perms`, `icon`, `create_dept`, `create_by`, `create_time`, `update_by`, `update_time`, `remark`)
SELECT 2099010100000000044, '内容修改', 2099010100000000041, 3, '#', '', NULL, 1, 0, 'F', '0', '0', 'cms:content:edit', '#', 103, 1, '2026-08-15 00:00:00', NULL, NULL, ''
WHERE NOT EXISTS (
    SELECT 1 FROM `sys_menu` WHERE `menu_id` = 2099010100000000044
    UNION
    SELECT 1 FROM `sys_menu` WHERE `perms` = 'cms:content:edit' AND `menu_type` = 'F'
);

-- 按钮：内容删除 (perms='cms:content:remove')
INSERT INTO `sys_menu` (`menu_id`, `menu_name`, `parent_id`, `order_num`, `path`, `component`, `query_param`, `is_frame`, `is_cache`, `menu_type`, `visible`, `status`, `perms`, `icon`, `create_dept`, `create_by`, `create_time`, `update_by`, `update_time`, `remark`)
SELECT 2099010100000000045, '内容删除', 2099010100000000041, 4, '#', '', NULL, 1, 0, 'F', '0', '0', 'cms:content:remove', '#', 103, 1, '2026-08-15 00:00:00', NULL, NULL, ''
WHERE NOT EXISTS (
    SELECT 1 FROM `sys_menu` WHERE `menu_id` = 2099010100000000045
    UNION
    SELECT 1 FROM `sys_menu` WHERE `perms` = 'cms:content:remove' AND `menu_type` = 'F'
);