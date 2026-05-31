document.addEventListener("DOMContentLoaded", () => {
    // DOM Elements
    const dropZone = document.getElementById("dropZone");
    const pdfFile = document.getElementById("pdfFile");
    const fileInfo = document.getElementById("fileInfo");
    const fileName = document.getElementById("fileName");
    const fileSize = document.getElementById("fileSize");
    const contextToggle = document.getElementById("contextToggle");
    const contextOptions = document.getElementById("contextOptions");
    const extractForm = document.getElementById("extractForm");
    const btnSubmit = document.getElementById("btnSubmit");
    const emptyState = document.getElementById("emptyState");
    const loader = document.getElementById("loader");
    const loaderText = document.getElementById("loaderText");
    const resultsList = document.getElementById("resultsList");
    const resultActions = document.getElementById("resultActions");
    const btnCopyToClipboard = document.getElementById("btnCopyToClipboard");
    const btnDownloadJson = document.getElementById("btnDownloadJson");
    const btnDownloadPdf = document.getElementById("btnDownloadPdf");

    let currentHighlights = [];
    let compiledPdfPath = null;

    // --- Toast Notification Helper ---
    function showToast(message, type = "success") {
        const container = document.getElementById("toastContainer");
        const toast = document.createElement("div");
        toast.className = `toast toast-${type}`;
        
        let icon = "fa-circle-check";
        if (type === "error") icon = "fa-circle-xmark";
        if (type === "warning") icon = "fa-triangle-exclamation";
        
        toast.innerHTML = `
            <i class="fa-solid ${icon} toast-icon-${type}"></i>
            <span>${message}</span>
        `;
        
        container.appendChild(toast);
        
        // Auto remove
        setTimeout(() => {
            toast.style.opacity = "0";
            toast.style.transform = "translateX(-20px)";
            toast.style.transition = "all 0.3s ease";
            setTimeout(() => toast.remove(), 300);
        }, 4000);
    }

    // --- Drag and Drop File Handlers ---
    dropZone.addEventListener("click", () => pdfFile.click());

    pdfFile.addEventListener("change", (e) => {
        if (e.target.files.length > 0) {
            handleSelectedFile(e.target.files[0]);
        }
    });

    dropZone.addEventListener("dragover", (e) => {
        e.preventDefault();
        dropZone.classList.add("dragover");
    });

    dropZone.addEventListener("dragleave", () => {
        dropZone.classList.remove("dragover");
    });

    dropZone.addEventListener("drop", (e) => {
        e.preventDefault();
        dropZone.classList.remove("dragover");
        if (e.dataTransfer.files.length > 0) {
            const file = e.dataTransfer.files[0];
            if (file.type === "application/pdf" || file.name.endsWith(".pdf")) {
                pdfFile.files = e.dataTransfer.files;
                handleSelectedFile(file);
            } else {
                showToast("الرجاء اختيار ملف PDF صالح فقط", "error");
            }
        }
    });

    function handleSelectedFile(file) {
        fileName.textContent = file.name;
        const sizeMB = (file.size / (1024 * 1024)).toFixed(2);
        fileSize.textContent = `${sizeMB} ميجابايت`;
        
        fileInfo.style.display = "flex";
        dropZone.querySelector(".file-icon").style.display = "none";
        dropZone.querySelector(".drop-zone-text").style.display = "none";
    }

    // --- Toggle Form Elements ---
    contextToggle.addEventListener("change", (e) => {
        contextOptions.style.display = e.target.checked ? "block" : "none";
    });

    // --- Submit Form and Extract Highlights ---
    extractForm.addEventListener("submit", async (e) => {
        e.preventDefault();
        
        if (pdfFile.files.length === 0) {
            showToast("الرجاء تحميل ملف PDF أولاً", "warning");
            return;
        }

        const taskId = "task_" + Date.now() + "_" + Math.random().toString(36).substr(2, 9);
        const formData = new FormData();
        formData.append("pdf", pdfFile.files[0]);
        formData.append("task_id", taskId);
        formData.append("context", contextToggle.checked);
        formData.append("context_margin", document.getElementById("contextMargin").value);
        formData.append("merge_threshold", document.getElementById("mergeThreshold").value);

        // Update UI states
        emptyState.style.display = "none";
        resultsList.style.display = "none";
        resultActions.style.display = "none";
        btnDownloadPdf.style.display = "none";
        compiledPdfPath = null;
        loader.style.display = "flex";
        btnSubmit.disabled = true;
        
        // Progress UI initialization
        const loaderProgressContainer = document.getElementById("loaderProgressContainer");
        const loaderProgressText = document.getElementById("loaderProgressText");
        const loaderProgressBar = document.getElementById("loaderProgressBar");
        
        loaderProgressContainer.style.display = "block";
        loaderProgressText.textContent = "جاري تهيئة عملية الاستخراج...";
        loaderProgressBar.style.width = "0%";

        loaderText.textContent = "جاري تحميل وقراءة مستند PDF...";

        // Poll progress API every 700ms
        let pollInterval = setInterval(async () => {
            try {
                const progressRes = await fetch(`/api/progress?task_id=${taskId}`);
                if (progressRes.ok) {
                    const progressData = await progressRes.json();
                    if (progressData.percent !== undefined) {
                        const pct = progressData.percent;
                        loaderProgressBar.style.width = `${pct}%`;
                        loaderProgressText.textContent = `جاري قراءة صفحات الكتاب: ${progressData.current} من ${progressData.total} (${pct}%)`;
                    } else if (progressData.total > 0) {
                        const pct = Math.round((progressData.current / progressData.total) * 100);
                        loaderProgressText.textContent = `جاري معالجة الصفحة ${progressData.current} من ${progressData.total}`;
                        loaderProgressBar.style.width = `${pct}%`;
                    }
                }
            } catch (err) {
                console.error("Error polling progress:", err);
            }
        }, 700);

        try {
            const res = await fetch("/api/extract", {
                method: "POST",
                body: formData
            });
            
            clearInterval(pollInterval);
            loaderProgressContainer.style.display = "none";
            
            if (!res.ok) {
                const errData = await res.json();
                throw new Error(errData.error || "فشلت عملية استخراج الاقتباسات");
            }
            
            const data = await res.json();
            currentHighlights = data.highlights || [];
            compiledPdfPath = data.compiled_pdf_path || null;
            
            renderResults(currentHighlights);
            showToast(`تمت المعالجة بنجاح! تم استخراج ${currentHighlights.length} اقتباسات.`);
            
        } catch (error) {
            clearInterval(pollInterval);
            loaderProgressContainer.style.display = "none";
            showToast(error.message || "حدث خطأ غير متوقع أثناء المعالجة", "error");
            loader.style.display = "none";
            emptyState.style.display = "flex";
        } finally {
            btnSubmit.disabled = false;
        }
    });

    // --- Render Results to UI ---
    function renderResults(highlights) {
        loader.style.display = "none";
        resultsList.innerHTML = "";
        
        if (highlights.length === 0) {
            emptyState.innerHTML = `
                <i class="fa-solid fa-face-meh empty-icon"></i>
                <h3>لم يتم العثور على أي تظليلات (Highlights)</h3>
                <p>تأكد من أن الملف يحتوي بالفعل على تظليلات نصوص ملونة وحاول مرة أخرى.</p>
            `;
            emptyState.style.display = "flex";
            return;
        }

        resultsList.style.display = "flex";
        resultActions.style.display = "flex";
        if (compiledPdfPath) {
            btnDownloadPdf.style.display = "inline-flex";
        } else {
            btnDownloadPdf.style.display = "none";
        }

        highlights.forEach((item, index) => {
            const card = document.createElement("div");
            card.className = "result-card";
            
            const highlightId = index + 1;
            
            let cropHtml = "";
            if (item.image_path) {
                const imgSrc = `/${item.image_path}`;
                cropHtml = `
                    <div class="crop-preview-container">
                        <img src="${imgSrc}" alt="قصاصة الاقتباس المرئية" class="crop-preview" onclick="window.open('${imgSrc}', '_blank')">
                    </div>
                `;
            }

            let contextHtml = "";
            if (item.context) {
                contextHtml = `
                    <div class="text-section">
                        <span class="text-label">الفقرة المحيطة (الكاملة):</span>
                        <p class="highlight-context">${item.context}</p>
                    </div>
                `;
            }

            card.innerHTML = `
                <div class="result-card-header">
                    <div class="result-meta">
                        <span><i class="fa-solid fa-hashtag"></i>اقتباس ${highlightId}</span>
                        <span><i class="fa-solid fa-file-lines"></i>صفحة ${item.page}</span>
                    </div>
                    <button class="btn-card-copy tooltip" data-tooltip="نسخ الاقتباس الحالي" data-index="${index}">
                        <i class="fa-solid fa-copy"></i>
                    </button>
                </div>
                <div class="result-card-body">
                    ${cropHtml}
                    <div class="result-texts">
                        <div class="text-section">
                            <span class="text-label">النص المستخرج:</span>
                            <p class="highlight-quote">${item.text}</p>
                        </div>
                        ${contextHtml}
                    </div>
                </div>
            `;
            
            const copyBtn = card.querySelector(".btn-card-copy");
            copyBtn.addEventListener("click", () => {
                const textToCopy = item.text + (item.context ? `\n\nالسياق:\n${item.context}` : "");
                navigator.clipboard.writeText(textToCopy).then(() => {
                    showToast(`تم نسخ الاقتباس ${highlightId} للمحفظة`);
                }).catch(() => {
                    showToast("فشل نسخ النص للمحفظة", "error");
                });
            });

            resultsList.appendChild(card);
        });
    }

    // --- Bulk Result Actions ---
    btnCopyToClipboard.addEventListener("click", () => {
        if (currentHighlights.length === 0) return;
        
        let allText = currentHighlights.map((item, idx) => {
            return `--- اقتباس ${idx + 1} (صفحة ${item.page}) ---\nالنص:\n${item.text}` + 
                   (item.context ? `\n\nالسياق:\n${item.context}` : "");
        }).join("\n\n\n");
        
        navigator.clipboard.writeText(allText).then(() => {
            showToast("تم نسخ كافة الاقتباسات للمحفظة بنجاح");
        }).catch(() => {
            showToast("فشل نسخ الاقتباسات", "error");
        });
    });

    btnDownloadJson.addEventListener("click", () => {
        if (currentHighlights.length === 0) return;
        
        const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(currentHighlights, null, 2));
        const downloadAnchor = document.createElement("a");
        downloadAnchor.setAttribute("href", dataStr);
        downloadAnchor.setAttribute("download", `${fileName.textContent.replace(".pdf", "")}_highlights.json`);
        document.body.appendChild(downloadAnchor);
        downloadAnchor.click();
        downloadAnchor.remove();
        showToast("جاري تحميل ملف JSON للنتائج");
    });

    btnDownloadPdf.addEventListener("click", () => {
        if (!compiledPdfPath) return;
        
        const downloadAnchor = document.createElement("a");
        downloadAnchor.setAttribute("href", `/${compiledPdfPath}`);
        downloadAnchor.setAttribute("download", `${fileName.textContent.replace(".pdf", "")}_images.pdf`);
        document.body.appendChild(downloadAnchor);
        downloadAnchor.click();
        downloadAnchor.remove();
        showToast("جاري تحميل ملف PDF للصور");
    });
});
