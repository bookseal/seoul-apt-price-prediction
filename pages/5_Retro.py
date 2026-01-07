import streamlit as st
from datetime import datetime


def render_retro_page() -> None:
    """Render a reflection section to document decisions and next steps."""
    st.title("Retro (Reflection)")

    st.markdown(
        """
### 📌 What did we build?
- Started with Seoul apartment transaction data
- Avoided loading large CSVs directly
- Generated a **100k stratified-sampled Parquet file**
- Achieved fast-loading structure in Streamlit

---

### 🤔 Why this design?
- Direct CSV loading → slow and memory-heavy
- Parquet → columnar format + fast I/O
- Stratified sampling → preserves district and year distribution
- At MVP stage, **architecture matters more than accuracy**

---

### ⚠️ Trade-offs & Known Limitations
- Sample data has statistical limitations for real analysis
- Some columns remain unused
- Year/district parsing depends on data format

---

### 🔜 Next Steps
- [ ] Price distribution visualization (histogram)
- [ ] Average price by district table
- [ ] Add year filter UI
- [ ] Experiment with sampling strategies (uniform vs. proportional)

---

### 🧠 Personal Lessons
- Pandas method chaining takes practice
- Verbose code → gradual compression strategy works well
- This project is **fundamentally a portfolio architecture exercise**
"""
    )

    # Optional: allow user to append more notes and export
    st.subheader("📝 Add More Notes (Optional)")
    st.caption("💡 Tip: On mobile, just tap Enter to create new lines. On desktop, use Shift+Enter for line breaks.")
    extra_notes = st.text_area(
        "Append details to this retro (won't change the on-screen content)",
        placeholder="Write additional learnings, decisions, or to-dos...",
        height=100,
    )

    # Compose markdown for download (base content + extra notes if any)
    base_md = """
### 📌 What did we build?
- Started with Seoul apartment transaction data
- Avoided loading large CSVs directly
- Generated a **100k stratified-sampled Parquet file**
- Achieved fast-loading structure in Streamlit

---

### 🤔 Why this design?
- Direct CSV loading → slow and memory-heavy
- Parquet → columnar format + fast I/O
- Stratified sampling → preserves district and year distribution
- At MVP stage, **architecture matters more than accuracy**

---

### ⚠️ Trade-offs & Known Limitations
- Sample data has statistical limitations for real analysis
- Some columns remain unused
- Year/district parsing depends on data format

---

### 🔜 Next Steps
- [ ] Price distribution visualization (histogram)
- [ ] Average price by district table
- [ ] Add year filter UI
- [ ] Experiment with sampling strategies (uniform vs. proportional)

---

### 🧠 Personal Lessons
- Pandas method chaining takes practice
- Verbose code → gradual compression strategy works well
- This project is **fundamentally a portfolio architecture exercise**
""".strip()

    if extra_notes.strip():
        base_md += "\n\n---\n\n### ➕ Additional Notes (Session)\n" + extra_notes.strip()

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"retro_{ts}.md"
    st.download_button(
        label="⬇️ Download Retro as Markdown",
        data=base_md.encode("utf-8"),
        file_name=filename,
        mime="text/markdown",
    )


def main() -> None:
    """Entry point for this page."""
    render_retro_page()


if __name__ == "__main__":
    main()
