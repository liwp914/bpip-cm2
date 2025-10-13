#!/bin/bash

# =============================================
# IP标记文件上传脚本 (支持多目录)
# 功能：同时处理 ip-info/443 和 ip-info/mrked 目录下的标记文件
# =============================================

# 从环境变量获取配置
CF_DOMAIN="${CF_DOMAIN}"
CF_TOKEN="${CF_TOKEN}"

# 定义要处理的目录和文件模式
# 格式：目录路径:文件匹配模式
DIRECTORY_PATTERNS=(
    "ip-info/443:*_marked.txt"
    "ip-info/mrked:*2_mrked.txt"
)

# 其他配置
MAX_RECORDS_PER_FILE="${MAX_RECORDS_PER_FILE:-10}"
UPLOAD_DELAY="${UPLOAD_DELAY:-1}"

# 检查必要的环境变量
if [ -z "$CF_DOMAIN" ] || [ -z "$CF_TOKEN" ]; then
    echo "错误: 必须设置 CF_DOMAIN 和 CF_TOKEN 环境变量"
    exit 1
fi

# 自定义URL编码函数
urlencode() {
    local string="$1"
    local encoded=""
    local pos c o

    [ -z "$string" ] && return

    pos=0
    while [ $pos -lt ${#string} ]; do
        c=${string:$pos:1}
        case "$c" in
            [-_.~a-zA-Z0-9]) o="$c" ;;
            *) o=$(printf '%%%02X' "'$c") ;;
        esac
        encoded+="$o"
        pos=$((pos + 1))
    done

    echo "$encoded"
}

# 处理单个目录的函数
process_directory() {
    local dir="$1"
    local pattern="$2"
    
    echo "检查目录: $dir (模式: $pattern)"
    
    # 检查目录是否存在
    if [ ! -d "$dir" ]; then
        echo "警告: 目录不存在: $dir，跳过"
        return 1
    fi
    
    # 检查目录是否可读
    if [ ! -r "$dir" ]; then
        echo "错误: 目录不可读: $dir，跳过"
        return 1
    fi
    
    local file_count=0
    local success_count=0
    
    echo "开始在目录 $dir 中搜索文件 (模式: $pattern)"
    
    # 使用find命令更可靠地查找文件[5,7](@ref)
    while IFS= read -r -d '' filename; do
        if [ -f "$filename" ]; then
            ((file_count++))
            echo "处理文件 ($file_count): $(basename "$filename")"
            
            # 获取文件名（不含路径）
            local filename_only=$(basename "$filename")
            
            # 读取文件的前N行内容并进行Base64编码
            local base64_text
            base64_text=$(head -n "$MAX_RECORDS_PER_FILE" "$filename" | base64 -w 0 2>/dev/null)
            
            if [ $? -ne 0 ] || [ -z "$base64_text" ]; then
                echo "✗ 文件读取或编码失败: $filename_only"
                continue
            fi
            
            # URL编码文件名
            local filename_url
            filename_url=$(urlencode "$filename_only")
            
            # 构建URL
            local url="https://${CF_DOMAIN}/${filename_url}?token=${CF_TOKEN}&b64=${base64_text}"
            
            # 使用curl发送请求[1](@ref)
            if curl -s -f --max-time 30 "$url" -o /dev/null; then
                echo "✓ 文件上传成功: $filename_only"
                ((success_count++))
            else
                echo "✗ 文件上传失败: $filename_only"
            fi
            
            # 添加延迟避免速率限制
            if [ "$UPLOAD_DELAY" -gt 0 ] && [ $file_count -lt $(find "$dir" -maxdepth 1 -name "$pattern" -type f | wc -l) ]; then
                sleep "$UPLOAD_DELAY"
            fi
        fi
    done < <(find "$dir" -maxdepth 1 -name "$pattern" -type f -print0 2>/dev/null)
    
    if [ $file_count -eq 0 ]; then
        echo "在目录 $dir 中没有找到匹配的文件 (模式: $pattern)"
        # 调试信息：显示目录内容
        echo "目录内容:"
        ls -la "$dir" 2>/dev/null | head -10 || echo "无法列出目录内容"
    else
        echo "目录 $dir 处理完成: $success_count/$file_count 个文件上传成功"
    fi
    
    return 0
}

echo "开始IP标记文件上传处理"
echo "目标目录配置:"
for pattern_config in "${DIRECTORY_PATTERNS[@]}"; do
    IFS=':' read -r dir pattern <<< "$pattern_config"
    echo "  - 目录: $dir, 文件模式: $pattern"
done
echo ""

total_processed=0
total_success=0

# 遍历所有目录配置[4,6](@ref)
for pattern_config in "${DIRECTORY_PATTERNS[@]}"; do
    IFS=':' read -r dir pattern <<< "$pattern_config"
    
    echo "=============================================="
    echo "开始处理目录: $dir"
    echo "=============================================="
    
    # 处理当前目录
    process_directory "$dir" "$pattern"
    
    echo ""
done

echo "=============================================="
echo "所有目录处理完成"
echo "=============================================="
