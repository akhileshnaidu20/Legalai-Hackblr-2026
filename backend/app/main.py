import os, sys
sys.path.insert(0, os.path.expanduser("~/hackblr-legal-ai"))
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from dotenv import load_dotenv
load_dotenv(os.path.expanduser("~/hackblr-legal-ai/.env"))
from backend.app.agents.legal_agent import LegalAgent

app = FastAPI(title="LegalAI Agent API — HackBLR 2026")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
agent = LegalAgent()

class ChatRequest(BaseModel):
    message: str

@app.get("/")
def root():
    return {"status":"ok","service":"LegalAI Agent — HackBLR 2026"}

@app.post("/chat")
async def chat(request: ChatRequest):
    try:
        result = agent.process_message(request.message)
        return {"intent":result.get("intent"),"response":result.get("response",""),"file_path":result.get("file_path"),"disclaimer":result.get("disclaimer")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analyze-contract")
async def analyze_contract_endpoint(file: UploadFile = File(...)):
    content = await file.read()
    text = content.decode("utf-8", errors="ignore")
    result = agent.handle_contract_analysis(text)
    return {"analysis":result["analysis"],"report_path":result.get("report_path"),"disclaimer":result["disclaimer"]}

@app.get("/download/{filename}")
async def download_file(filename: str):
    fp = os.path.join(os.path.expanduser("~/hackblr-legal-ai/generated_docs"), filename)
    if os.path.exists(fp):
        return FileResponse(fp, filename=filename)
    raise HTTPException(status_code=404, detail="File not found")

@app.post("/reset")
async def reset():
    global agent
    agent = LegalAgent()
    return {"status":"conversation reset"}

@app.post("/vapi/webhook")
async def vapi_webhook(request: dict):
    mt = request.get("message",{}).get("type","")
    if mt == "function-call":
        fn = request["message"]["functionCall"]["name"]
        params = request["message"]["functionCall"]["parameters"]
        if fn == "legal_query":
            result = agent.process_message(params.get("query",""))
            return {"result":result["response"]}
    return {"result":"I help with legal questions, contract analysis, and document drafting."}
