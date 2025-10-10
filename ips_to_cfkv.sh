#!/bin/bash

# 从环境变量获取配置
CF_DOMAIN="${CF_DOMAIN}"
CF_TOKEN="${CF_TOKEN}"
FOLDER_PATH="${FOLDER_PATH:-ip-info/443}"  # 默认路径

# 检查必要的环境变量
if [ -z "$CF_DOMAIN" ] || [ -z "$CF_TOKEN" ]; then
    echo "错误: 必须设置 CF_DOMAIN 和 CF_TOKEN 环境变量"
    exit 1
fi

# 检查目标文件夹是否存在
if [ ! -d "$FOLDER_PATH" ]; then
    echo "错误: 文件夹 $FOLDER_PATH 不存在"
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

echo "开始上传文件从文件夹: $FOLDER_PATH"

# 遍历文件夹中所有匹配 *_marked.txt 的文件
for FILENAME in "${FOLDER_PATH}"/*_marked.txt; do
    # 检查文件是否存在
    if [ ! -f "$FILENAME" ]; then
        echo "文件不存在: $FILENAME"
        continue
    fi

    # 获取文件名（不含路径）
    FILENAME_ONLY=$(basename "$FILENAME")

    # 读取文件的前10行内容并进行Base64编码
    BASE64_TEXT=$(head -n 10 "$FILENAME" | base64 -w 0)

    # URL编码文件名
    FILENAME_URL=$(urlencode "$FILENAME_ONLY")

    # 构建URL
    URL="https://${CF_DOMAIN}/${FILENAME_URL}?token=${CF_TOKEN}&b64=${BASE64_TEXT}"

    echo "上传文件: $FILENAME"

    # 使用curl发送请求
    if curl -s -f "$URL" -o /dev/null; then
        echo "✓ 文件 $FILENAME 上传成功"
    else
        echo "✗ 文件 $FILENAME 上传失败"
    fi
done

echo "所有文件上传完成"
