package org.ruoyi.system.service;

import org.ruoyi.common.mybatis.core.page.PageQuery;
import org.ruoyi.common.mybatis.core.page.TableDataInfo;
import org.ruoyi.system.domain.bo.CmsContentBo;
import org.ruoyi.system.domain.vo.CmsContentVo;

import java.util.List;

/**
 * 内容管理 服务层
 *
 * @author ruoyi
 */
public interface ICmsContentService {

    /**
     * 分页查询内容管理列表
     */
    TableDataInfo<CmsContentVo> selectPageCmsContentList(CmsContentBo bo, PageQuery pageQuery);

    /**
     * 查询内容信息
     */
    CmsContentVo selectCmsContentById(Long contentId);

    /**
     * 查询内容列表
     */
    List<CmsContentVo> selectCmsContentList(CmsContentBo bo);

    /**
     * 新增内容
     */
    int insertCmsContent(CmsContentBo bo);

    /**
     * 修改内容
     */
    int updateCmsContent(CmsContentBo bo);

    /**
     * 删除内容信息
     */
    int deleteCmsContentById(Long contentId);

    /**
     * 批量删除内容信息
     */
    int deleteCmsContentByIds(Long[] contentIds);
}