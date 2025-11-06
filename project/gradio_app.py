import gradio as gr
from orchestrator import Orchestrator

orch = Orchestrator()

def run_agentic_pipeline(company_name, company_number, postcode, sic_codes):
    try:
        sic_list = [s.strip() for s in sic_codes.split(",") if s.strip()]
        result = orch.run_one(company_name, company_number, postcode, sic_list)
        return result
    except Exception as e:
        return {"error": str(e)}

demo = gr.Interface(
    fn=run_agentic_pipeline,
    inputs=[
        gr.Textbox(label="Company Name", placeholder="Horizon Web Limited"),
        gr.Textbox(label="Company Number", placeholder="12529936"),
        gr.Textbox(label="Postcode", placeholder="SK1 1EB"),
        gr.Textbox(label="SIC Codes (comma-separated)", placeholder="62012, 62020"),
    ],
    outputs=gr.JSON(label="Agentic Result"),
    title="Agentic Company Website Finder",
    description="An LLM-powered multi-agent system that searches, scrapes, and verifies company websites."
)

if __name__ == "__main__":
    demo.launch()
