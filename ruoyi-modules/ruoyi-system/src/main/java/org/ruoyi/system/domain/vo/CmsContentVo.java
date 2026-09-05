package org.ruoyi.system.domain.vo;

import org.ruoyi.common.translation.annotation.Translation;
import org.ruoyi.common.translation.constant.TransConstant;
import org.ruoyi.system.domain.CmsContent;
import io.github.linpeilie.annotations.AutoMapper;
import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

/**
 * 内容管理视图对象 cms_content
 *
 * @author ruoyi
 */
@Data
@AutoMapper(target = CmsContent.class)
public class CmsContentVo implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * 内容ID
     */
    private Long contentId;

    /**
     * 内容标题
     */
    private String title;

    /**
     * 摘要
     */
    private String summary;

    /**
     * 正文内容
     */
    private String content;

    /**
     * 封面图片
     */
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
    private String remark;

    /**
     * 创建者
     */
    private Long createBy;

    /**
     * 创建人名称
     */
    @Translation(type = TransConstant.USER_ID_TO_NAME, mapper = "createBy")
    private String createByName;

    /**
     * 创建时间
     */
    private Date createTime;

}