import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from flask import Flask, request, jsonify

app = Flask(__name__)

# ---------- 1) 加载模型，准备推理 ----------
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model_id = "clapAI/modernBERT-base-multilingual-sentiment"
tokenizer = AutoTokenizer.from_pretrained(model_id)
# 注意：如果你的环境CPU不支持float16，可以去掉 torch_dtype=torch.float16
model = AutoModelForSequenceClassification.from_pretrained(model_id, torch_dtype=torch.float16)
model.to(device)
model.eval()
id2label = model.config.id2label

# ---------- 2) 提供 /api/sentiment 接口 ----------
@app.route("/api/sentiment", methods=["POST"])
def analyze_sentiment():
    data = request.json
    if not data or "text" not in data:
        return jsonify({"error": "No 'text' field provided"}), 400
    text = data["text"]
    try:
        inputs = tokenizer(text, return_tensors="pt").to(device)
        with torch.inference_mode():
            outputs = model(**inputs)
            pred_idx = outputs.logits.argmax(dim=-1).item()
        sentiment_str = id2label[pred_idx]
        return jsonify({"sentiment": sentiment_str})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---------- 3) 启动服务 ----------
if __name__ == "__main__":
    # 默认监听5001端口，可根据需要调整
    app.run(host="0.0.0.0", port=5001, debug=False)