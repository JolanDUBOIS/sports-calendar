window.openModifyFilterModal = function(filterId, onConfirm) {
    console.log("Opening modify filter modal for filterId:", filterId);
    const modalBody = document.getElementById("modify-filter-body");
    modalBody.innerHTML = ""; // clear previous content

    // clone the correct hidden template
    const template = document.getElementById(`filter-form-${filterId}`);
    if (!template) return;

    // Clone the node and remove its id
    const clone = template.cloneNode(true);
    clone.removeAttribute("id");
    clone.classList.remove("d-none");

    modalBody.appendChild(clone);

    // Remove previous listeners
    const modalEl = document.getElementById("modifyFilterModal");
    const confirmBtn = modalEl.querySelector("#modify-filter-confirm-btn");
    confirmBtn.replaceWith(confirmBtn.cloneNode(true));
    const newConfirmBtn = modalEl.querySelector("#modify-filter-confirm-btn");

    // Attach new listener
    newConfirmBtn.addEventListener("click", () => {
        const form = modalBody.querySelector("form");
        if (!form) return;
        const formData = new FormData(form);
        const data = {};
        for (const [key, value] of formData.entries()) {
            data[key] = value;
        }
        onConfirm(data);
        bootstrap.Modal.getInstance(modalEl).hide();
    });

    // show modal
    new bootstrap.Modal(modalEl).show();
}