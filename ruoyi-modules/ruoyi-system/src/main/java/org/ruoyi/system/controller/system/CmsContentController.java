package org.ruoyi.system.controller.system;

import cn.dev33.satoken.annotation.SaCheckPermission;
import lombok.RequiredArgsConstructor;
import org.ruoyi.common.core.domain.R;
import org.ruoyi.common.idempotent.annotation.RepeatSubmit;
import org.ruoyi.common.log.annotation.Log;
import org.ruoyi.common.log.enums.BusinessType;
import org.ruoyi.common.mybatis.core.page.PageQuery;
import org.ruoyi.common.mybatis.core.page.TableDataInfo;
import org.ruoyi.common.web.core.BaseController;
import org.ruoyi.system.domain.bo.CmsContentBo;
import org.ruoyi.system.domain.vo.CmsContentVo;
import org.ruoyi.system.service.ICmsContentService;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * 内容管理 信息操作处理
 *
 * @author ruoyi
 */
@Validated
@RequiredArgsConstructor
@RestController
@RequestMapping("/cms/content")
public class CmsContentController extends BaseController {

    private final ICmsContentService cmsContentService;

    /**
     * 获取内容管理列表
     */
    @SaCheckPermission("cms:content:list")
    @GetMapping("/list")
    public TableDataInfo<CmsContentVo> list(CmsContentBo bo, PageQuery pageQuery) {
        return cmsContentService.selectPageCmsContentList(bo, pageQuery);
    }

    /**
     * 根据内容编号获取详细信息
     */
    @SaCheckPermission("cms:content:query")
    @GetMapping(value = "/{contentId}")
    public R<CmsContentVo> getInfo(@PathVariable Long contentId) {
        return R.ok(cmsContentService.selectCmsContentById(contentId));
    }

    /**
     * 新增内容
     */
    @SaCheckPermission("cms:content:add")
    @Log(title = "内容管理", businessType = BusinessType.INSERT)
    @RepeatSubmit()
    @PostMapping
    public R<Void> add(@Validated @RequestBody CmsContentBo bo) {
        return toAjax(cmsContentService.insertCmsContent(bo));
    }

    /**
     * 修改内容
     */
    @SaCheckPermission("cms:content:edit")
    @Log(title = "内容管理", businessType = BusinessType.UPDATE)
    @RepeatSubmit()
    @PutMapping
    public R<Void> edit(@Validated @RequestBody CmsContentBo bo) {
        return toAjax(cmsContentService.updateCmsContent(bo));
    }

    /**
     * 删除内容
     */
    @SaCheckPermission("cms:content:remove")
    @Log(title = "内容管理", businessType = BusinessType.DELETE)
    @DeleteMapping("/{contentIds}")
    public R<Void> remove(@PathVariable Long[] contentIds) {
        return toAjax(cmsContentService.deleteCmsContentByIds(contentIds));
    }
}