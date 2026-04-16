import os, sys, json, concurrent.futures
from dotenv import load_dotenv
from groq import Groq
load_dotenv(os.path.expanduser("~/hackblr-legal-ai/.env"))
sys.path.insert(0, os.path.expanduser("~/hackblr-legal-ai"))
from backend.app.tools.legal_search import search_legal_db
from backend.app.tools.contract_analyzer import analyze_contract
from backend.app.tools.doc_generator import generate_legal_notice, generate_contract_review_report

groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# ============================================================
# LAYER 1: 5 SPECIALIST AGENTS (run in parallel)
# ============================================================

RESEARCHER_PROMPT = """You are a LEGAL RESEARCHER specialist for Indian law.
Your job: Find and cite exact legal sections, statutes, and case law relevant to the user's problem.

RULES:
- Cite specific section numbers (e.g., Section 420 IPC / Section 318 BNS)
- For criminal law, cite BOTH old (IPC/CrPC/Evidence Act) and new (BNS/BNSS/BSA) sections
- New criminal codes replaced old codes on 1 July 2024
- Include the punishment/penalty prescribed for each offence
- Cite landmark Supreme Court judgments if relevant
- Be precise — only cite sections you are confident about
- Format each citation as: Section X of [Act Name] — [brief description]"""

PRACTICAL_PROMPT = """You are a PRACTICAL ADVISOR specialist for Indian law.
Your job: Give step-by-step actionable advice that a common person with ZERO legal knowledge can follow.

RULES:
- Number each step clearly (Step 1, Step 2, etc.)
- Include WHERE to go (which court, which office, which website, which police station)
- Include HOW MUCH it costs (court fees, lawyer fees, or if FREE)
- Include HOW LONG the process typically takes
- Include PHONE NUMBERS: police 100, women helpline 181, cyber crime 1930, NALSA 15100, consumer helpline 1800-11-4000
- Include WEBSITES: edaakhil.nic.in, cybercrime.gov.in, rtionline.gov.in where relevant
- Mention if FREE legal aid is available through NALSA (helpline 15100)
- Keep language simple — explain like talking to a 10th class student"""

LAW_ADVISOR_PROMPT = """You are a LAW ADVISOR specialist for Indian law.
Your job: Analyze the legal merits of the person's case and advise on the strength of their position.

RULES:
- Assess whether the person has a strong, moderate, or weak legal case
- Identify which legal provisions work IN THEIR FAVOUR
- Identify potential weaknesses or risks in their case
- Suggest what evidence they should collect and preserve
- Advise on whether to pursue court action, mediation, Lok Adalat, or consumer forum
- Mention time limitations (limitation period) for filing cases
- If multiple legal remedies exist, rank them from best to worst
- Be honest about chances — don't give false hope but don't discourage valid claims"""

RIGHTS_PROMPT = """You are a LEGAL RIGHTS specialist for Indian law.
Your job: Tell the person every RIGHT they have in their situation, including rights they probably don't know about.

RULES:
- Start each right with "You have the right to..."
- Cite the source of each right (Constitutional Article, Act, Section)
- Include fundamental rights under Constitution (Articles 14, 19, 21, 32 etc.)
- Include statutory rights under relevant Acts
- Include right to FREE legal aid (Section 12, Legal Services Authorities Act 1987)
- Include right to information (RTI Act 2005)
- Include rights specific to their situation (tenant rights, employee rights, consumer rights, women's rights)
- Mention if any of their rights are currently being VIOLATED
- Include protections available (e.g., no employer can fire you for filing a complaint)"""

LAW_ORDER_PROMPT = """You are a LAW AND ORDER specialist for Indian law.
Your job: Advise on criminal law aspects, police procedures, FIR filing, bail, and enforcement.

RULES:
- Determine if the situation involves any criminal offence (cognizable or non-cognizable)
- If cognizable: explain FIR process, police obligations under Section 154 CrPC / Section 173 BNSS
- If police refuses FIR: explain Zero FIR, complaint to SP, Section 156(3) CrPC / Section 175(3) BNSS magistrate order
- Explain bail provisions if relevant (regular bail, anticipatory bail, default bail)
- Mention arrest protections: rights of arrested person under Article 22, DK Basu guidelines
- For women: mention special provisions — women cannot be arrested after sunset/before sunrise, must be arrested by female officer
- Advise on whether to file FIR, private complaint, or both
- Include relevant cyber crime procedures if applicable (helpline 1930, cybercrime.gov.in)
- Explain the difference between civil remedy and criminal remedy for their situation"""

# ============================================================
# LAYER 2: SYNTHESIZER AGENT
# ============================================================

SYNTH_PROMPT = """You are the CHIEF LEGAL ADVISOR for LegalAI (HackBLR 2026).
You receive analysis from 5 specialist legal agents:
1. RESEARCHER — found relevant statutes and case law
2. PRACTICAL ADVISOR — gave step-by-step actionable guidance
3. LAW ADVISOR — assessed legal merits and case strength
4. RIGHTS SPECIALIST — identified all applicable rights
5. LAW & ORDER SPECIALIST — advised on criminal aspects and police procedures

Your job: Combine all 5 into ONE clear, comprehensive, well-structured answer.

OUTPUT STRUCTURE:
1. Brief empathetic acknowledgment (1 line — show you understand their problem)
2. YOUR RIGHTS — what rights the person has (from Rights Specialist)
3. LEGAL POSITION — how strong their case is (from Law Advisor)
4. APPLICABLE LAWS — key sections with citations (from Researcher)
5. WHAT TO DO — numbered steps in order of priority (from Practical Advisor)
6. POLICE/CRIMINAL ASPECTS — if applicable (from Law & Order Specialist)
7. IMPORTANT CONTACTS — relevant helpline numbers and websites
8. DISCLAIMER — "This is AI-generated legal guidance, not professional legal advice. For your specific situation, consult a qualified lawyer. Free legal aid: NALSA helpline 15100."

RULES:
- If specialists disagree, go with the more conservative/accurate one
- Remove redundancy — don't repeat the same point from different agents
- Keep language SIMPLE — the user may have low literacy
- If the user asked in Hindi/Kannada/Telugu/Tamil/Malayalam, respond in the SAME language
- Keep all section numbers and act names in English even in regional language responses
- Total response should be comprehensive but not overwhelming — aim for clarity"""

def detect_language(text):
    for char in text:
        c = ord(char)
        if 0x0900 <= c <= 0x097F: return "hi"
        if 0x0C80 <= c <= 0x0CFF: return "kn"
        if 0x0C00 <= c <= 0x0C7F: return "te"
        if 0x0B80 <= c <= 0x0BFF: return "ta"
        if 0x0D00 <= c <= 0x0D7F: return "ml"
    return "en"

def run_specialist(name, prompt, query, context):
    try:
        r = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"system","content":prompt},
                      {"role":"user","content":f"LEGAL CONTEXT FROM DATABASE:\n{context}\n\nUSER'S PROBLEM: {query}\n\nProvide your specialist analysis."}],
            temperature=0.3, max_tokens=800)
        return {"agent":name,"output":r.choices[0].message.content,"status":"ok"}
    except Exception as e:
        return {"agent":name,"output":str(e)[:200],"status":"error"}

def synthesize(query, outputs, lang):
    agents_text = ""
    for o in outputs:
        agents_text += f"\n{'='*30}\n{o['agent'].upper()}:\n{'='*30}\n{o['output']}\n"
    lang_map = {"hi":"Hindi","kn":"Kannada","te":"Telugu","ta":"Tamil","ml":"Malayalam"}
    lang_note = f"\nIMPORTANT: The user asked in {lang_map[lang]}. Respond in {lang_map[lang]} but keep section numbers and act names in English." if lang != "en" else ""
    try:
        r = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"system","content":SYNTH_PROMPT},
                      {"role":"user","content":f"USER'S QUESTION: {query}\n\n5 SPECIALIST ANALYSES:{agents_text}{lang_note}\n\nNow synthesize into ONE comprehensive answer following the output structure."}],
            temperature=0.3, max_tokens=3000)
        return r.choices[0].message.content
    except Exception as e:
        for o in outputs:
            if o["status"] == "ok": return o["output"]
        return f"Error: {e}"

# ============================================================
# MAIN AGENT CLASS
# ============================================================

class LegalAgent:
    def __init__(self):
        self.conversation_history = []

    def classify_intent(self, msg):
        m = msg.lower()
        if any(w in m for w in ["analyze contract","review contract","check contract","clause","risky"]): return "contract_analysis"
        if any(w in m for w in ["draft","generate","create notice","legal notice","write a letter","prepare document"]): return "document_generation"
        return "legal_research"

    def handle_legal_research(self, query):
        lang = detect_language(query)
        results = search_legal_db(query, top_k=7)
        context = ""
        for i, r in enumerate(results, 1):
            tag = " [NEW LAW]" if r.get("law_status") == "new" else ""
            context += f"{i}. [{r['source']}]{tag}\n   {r['content'][:400]}\n\n"

        specialists = [
            ("Researcher", RESEARCHER_PROMPT),
            ("Practical Advisor", PRACTICAL_PROMPT),
            ("Law Advisor", LAW_ADVISOR_PROMPT),
            ("Rights Specialist", RIGHTS_PROMPT),
            ("Law & Order Specialist", LAW_ORDER_PROMPT),
        ]

        outputs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            futs = {ex.submit(run_specialist, n, p, query, context): n for n,p in specialists}
            for f in concurrent.futures.as_completed(futs):
                outputs.append(f.result())

        return synthesize(query, outputs, lang)

    def handle_contract_analysis(self, text):
        result = analyze_contract(text)
        report = generate_contract_review_report("User Contract", result["analysis"],
            ["Have a lawyer review HIGH risk clauses","Ensure compliance with Indian Contract Act 1872","Verify jurisdiction clauses"])
        return {"analysis":result["analysis"],"report_path":report,"disclaimer":result["disclaimer"]}

    def handle_document_generation(self, msg):
        try:
            r = groq_client.chat.completions.create(model="llama-3.3-70b-versatile",
                messages=[{"role":"user","content":f'Extract from this and return ONLY raw JSON: {{"sender_name":"...","sender_address":"...","recipient_name":"...","recipient_address":"...","subject":"...","body":"...","demands":["..."]}}\nUse "[PLACEHOLDER]" for missing info.\n\nMessage: {msg}'}],
                temperature=0.1, max_tokens=1000)
            t = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
            p = json.loads(t)
        except: p = {"sender_name":"[YOUR NAME]","sender_address":"[YOUR ADDRESS]","recipient_name":"[RECIPIENT]","recipient_address":"[ADDRESS]","subject":"Legal Notice","body":msg,"demands":["Comply within 15 days"]}
        fp = generate_legal_notice(p.get("sender_name","[NAME]"),p.get("sender_address","[ADDR]"),p.get("recipient_name","[RECIP]"),p.get("recipient_address","[ADDR]"),p.get("subject","Legal Notice"),p.get("body",msg),p.get("demands",[]))
        return {"message":"Legal notice generated.","file_path":fp,"disclaimer":"AI draft. Have a lawyer review before sending."}

    def process_message(self, msg):
        self.conversation_history.append({"role":"user","content":msg})
        intent = self.classify_intent(msg)
        if intent == "contract_analysis":
            result = self.handle_contract_analysis(msg); resp = result["analysis"]
        elif intent == "document_generation":
            result = self.handle_document_generation(msg); resp = result["message"]
        else:
            resp = self.handle_legal_research(msg); result = {"response":resp}
        self.conversation_history.append({"role":"assistant","content":resp})
        return {"intent":intent,"response":resp,**result}
