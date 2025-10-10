#!/bin/bash

# 切换到ip-info/443目录（根据新目录结构调整）
cd ip-info/443 || { echo "无法切换到ip-info/443目录，脚本退出。" >&2; exit 1; }

# 自定义URL编码函数
urlencode() {
    local string="$1"  # 修正：使用传入的参数而不是固定值
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

# 从环境变量中获取 CF_DOMAIN 和 CF_TOKEN
CF_DOMAIN="${CF_DOMAIN}"
CF_TOKEN="${CF_TOKEN}"

# 获取传入的所有文件名参数
files=("$@")

# 循环处理每个文件
for FILENAME in "${files[@]}"; do
    # 只处理带_marked后缀的文件
    if [[ "$FILENAME" != *"_marked.txt" ]]; then
        echo "跳过非标记文件: $FILENAME"
        continue
    fi
    
    # 判断文件是否存在
    if [ -f "$FILENAME" ]; then
        # 逐行读取文件内容进行Base64编码
        BASE64_TEXT=""
        line_count=0
        
        while IFS= read -r line; do
            # 只处理前10行（根据项目需求）
            if [ $line_count -lt 10 ]; then
                BASE64_TEXT+=$(echo -n "$line" | base64 -w 0)
                line_count=$((line_count + 1))
            else
                break
            fi
        done < "$FILENAME"
        
        # 对整个内容进行Base64编码
        BASE64_TEXT=$(echo -n "$BASE64_TEXT" | base64 -w 0)
        
        FILENAME_URL=$(urlencode "$FILENAME")
        URL="https://${CF_DOMAIN}/${FILENAME_URL}?token=${CF_TOKEN}&b64=${BASE64_TEXT}"

        # 使用curl发送请求
        echo "上传文件: $FILENAME"
        curl -s "$URL"
        response_code=$?
        
        if [ $response_code -eq 0 ]; then
            echo "文件 $FILENAME 上传成功"
        else
            echo "文件 $FILENAME 上传失败，错误码: $response_code"
        fi
    else
        echo "文件 $FILENAME 不存在，跳过处理。"
    fi
done

echo "所有文件上传完成。"