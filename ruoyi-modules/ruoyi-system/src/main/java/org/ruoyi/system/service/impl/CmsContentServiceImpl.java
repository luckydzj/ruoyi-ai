package org.ruoyi.system.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import org.ruoyi.common.core.utils.MapstructUtils;
import org.ruoyi.common.core.utils.StringUtils;
import org.ruoyi.common.mybatis.core.page.PageQuery;
import org.ruoyi.common.mybatis.core.page.TableDataInfo;
import org.ruoyi.system.domain.CmsContent;
import org.ruoyi.system.domain.bo.CmsContentBo;
import org.ruoyi.system.domain.vo.CmsContentVo;
import org.ruoyi.system.mapper.CmsContentMapper;
import org.ruoyi.system.service.ICmsContentService;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * 内容管理 服务层实现
 *
 * @author ruoyi
 */
@RequiredArgsConstructor
@Service
public class CmsContentServiceImpl implements ICmsContentService {

    private final CmsContentMapper baseMapper;

    @Override
    public TableDataInfo<CmsContentVo> selectPageCmsContentList(CmsContentBo bo, PageQuery pageQuery) {
        LambdaQueryWrapper<CmsContent> lqw = buildQueryWrapper(bo);
        Page<CmsContentVo> page = baseMapper.selectVoPage(pageQuery.build(), lqw);
        return TableDataInfo.build(page);
    }

    @Override
    public CmsContentVo selectCmsContentById(Long contentId) {
        return baseMapper.selectVoById(contentId);
    }

    @Override
    public List<CmsContentVo> selectCmsContentList(CmsContentBo bo) {
        LambdaQueryWrapper<CmsContent> lqw = buildQueryWrapper(bo);
        return baseMapper.selectVoList(lqw);
    }

    private LambdaQueryWrapper<CmsContent> buildQueryWrapper(CmsContentBo bo) {
        LambdaQueryWrapper<CmsContent> lqw = Wrappers.lambdaQuery();
        lqw.like(StringUtils.isNotBlank(bo.getTitle()), CmsContent::getTitle, bo.getTitle());
        lqw.eq(StringUtils.isNotBlank(bo.getStatus()), CmsContent::getStatus, bo.getStatus());
        lqw.eq(bo.getPublishTime() != null, CmsContent::getPublishTime, bo.getPublishTime());
        lqw.orderByAsc(CmsContent::getSortOrder);
        lqw.orderByDesc(CmsContent::getCreateTime);
        return lqw;
    }

    @Override
    public int insertCmsContent(CmsContentBo bo) {
        CmsContent content = MapstructUtils.convert(bo, CmsContent.class);
        return baseMapper.insert(content);
    }

    @Override
    public int updateCmsContent(CmsContentBo bo) {
        CmsContent content = MapstructUtils.convert(bo, CmsContent.class);
        return baseMapper.updateById(content);
    }

    @Override
    public int deleteCmsContentById(Long contentId) {
        return baseMapper.deleteById(contentId);
    }

    @Override
    public int deleteCmsContentByIds(Long[] contentIds) {
        return baseMapper.deleteByIds(Arrays.asList(contentIds));
    }
}