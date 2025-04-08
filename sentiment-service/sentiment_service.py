import torch
from flask import Flask, jsonify, request
from transformers import AutoModelForSequenceClassification, AutoTokenizer

app = Flask(__name__)

# ---------- 1) Load model for inference ----------
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model_id = "clapAI/modernBERT-base-multilingual-sentiment"
tokenizer = AutoTokenizer.from_pretrained(model_id)
# Note: If your CPU doesn't support float16, remove torch_dtype=torch.float16
model = AutoModelForSequenceClassification.from_pretrained(
    model_id, torch_dtype=torch.float16
)
model.to(device)
model.eval()
id2label = model.config.id2label


# ---------- 2) Provide /api/sentiment endpoint ----------
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


# ---------- 3) Start server ----------
if __name__ == "__main__":
    # Default port 5001, adjust as needed
    app.run(host="0.0.0.0", port=5001, debug=False)
