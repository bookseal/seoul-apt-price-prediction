import streamlit as st
from datetime import datetime


def render_retro_page() -> None:
    """Render a reflection section to document decisions and next steps."""
    st.title("Retro (Reflection)")

    st.markdown(
        """
### 📌 What did we build?
- 서울 아파트 거래 데이터에서
- 대규모 CSV를 직접 쓰지 않고
- **Stratified Sampling 기반 100k 샘플 Parquet**를 생성함
- Streamlit에서 빠르게 로딩 가능한 구조 확보

---

### 🤔 Why this design?
- CSV 직접 로딩 → 느리고 무거움
- Parquet → 컬럼 단위 로딩 + 빠른 I/O
- Stratified Sampling → 지역/연도 분포 유지
- MVP 단계에서는 **정확도보다 “구조”가 우선**

---

### ⚠️ Trade-offs & Known Limitations
- 샘플 데이터이므로 실제 통계 분석엔 한계 있음
- 일부 컬럼은 아직 사용하지 않음
- 연도/구 파싱 로직은 데이터 포맷에 의존적

---

### 🔜 Next Steps
- [ ] 가격 분포 시각화 (Histogram)
- [ ] 구별 평균 가격 테이블
- [ ] 연도 필터 UI 추가
- [ ] 샘플링 전략 실험 (균등 vs 비례)

---

### 🧠 Personal Notes
- pandas 체이닝은 익숙해질 필요 있음
- 풀어 쓴 코드 → 점진적 압축 전략이 효과적
- 이 프로젝트는 **포트폴리오용 구조 연습**이 핵심
"""
    )

    # Optional: allow user to append more notes and export
    st.subheader("📝 Add More Notes (Optional)")
    extra_notes = st.text_area(
        "Append details to this retro (won't change the on-screen content)",
        placeholder="Write additional learnings, decisions, or to-dos...",
        height=150,
    )

    # Compose markdown for download (base content + extra notes if any)
    base_md = """
### 📌 What did we build?
- 서울 아파트 거래 데이터에서
- 대규모 CSV를 직접 쓰지 않고
- **Stratified Sampling 기반 100k 샘플 Parquet**를 생성함
- Streamlit에서 빠르게 로딩 가능한 구조 확보

---

### 🤔 Why this design?
- CSV 직접 로딩 → 느리고 무거움
- Parquet → 컬럼 단위 로딩 + 빠른 I/O
- Stratified Sampling → 지역/연도 분포 유지
- MVP 단계에서는 **정확도보다 “구조”가 우선**

---

### ⚠️ Trade-offs & Known Limitations
- 샘플 데이터이므로 실제 통계 분석엔 한계 있음
- 일부 컬럼은 아직 사용하지 않음
- 연도/구 파싱 로직은 데이터 포맷에 의존적

---

### 🔜 Next Steps
- [ ] 가격 분포 시각화 (Histogram)
- [ ] 구별 평균 가격 테이블
- [ ] 연도 필터 UI 추가
- [ ] 샘플링 전략 실험 (균등 vs 비례)

---

### 🧠 Personal Notes
- pandas 체이닝은 익숙해질 필요 있음
- 풀어 쓴 코드 → 점진적 압축 전략이 효과적
- 이 프로젝트는 **포트폴리오용 구조 연습**이 핵심
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
