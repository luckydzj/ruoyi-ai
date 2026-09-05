package org.ruoyi.system.domain.bo;

import io.github.linpeilie.annotations.AutoMapper;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.ruoyi.common.core.xss.Xss;
import org.ruoyi.common.mybatis.core.domain.BaseEntity;
import org.ruoyi.system.domain.CmsContent;

import java.util.Date;

/**
 * 内容管理业务对象 cms_content
 *
 * @author ruoyi
 */
@Data
@EqualsAndHashCode(callSuper = true)
@AutoMapper(target = CmsContent.class, reverseConvertGenerate = false)
public class CmsContentBo extends BaseEntity {

    /**
     * 内容ID
     */
    private Long contentId;

    /**
     * 内容标题
     */
    @Xss(message = "内容标题不能包含脚本字符")
    @NotBlank(message = "内容标题不能为空")
    @Size(min = 0, max = 200, message = "内容标题不能超过{max}个字符")
    private String title;

    /**
     * 摘要
     */
    @Size(min = 0, max = 500, message = "摘要不能超过{max}个字符")
    private String summary;

    /**
     * 正文内容
     */
    private String content;

    /**
     * 封面图片
     */
    @Size(min = 0, max = 255, message = "封面图片路径不能超过{max}个字符")
    private String coverImage;

    /**
     * 状态（0草稿 1已发布 2下架）
     */
    private String status;

    /**
     * 发布时间
     */
    private Date publishTime;

    /**
     * 排序顺序
     */
    private Integer sortOrder;

    /**
     * 备注
     */
    @Size(min = 0, max = 500, message = "备注不能超过{max}个字符")
    private String remark;

    /**
     * 创建人名称
     */
    private String createByName;

}