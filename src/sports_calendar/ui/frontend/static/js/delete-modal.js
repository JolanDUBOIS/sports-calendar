window.openDeleteModal = function(message, onConfirm) {
    const modalEl = document.getElementById("deleteModal");
    const messageEl = modalEl.querySelector("#modal-message");
    const confirmBtn = modalEl.querySelector("#modal-confirm-btn");

    // Set the message
    messageEl.textContent = message;

    // Remove previous listeners
    confirmBtn.replaceWith(confirmBtn.cloneNode(true));
    const newConfirmBtn = modalEl.querySelector("#modal-confirm-btn");

    // Attach new listener
    newConfirmBtn.addEventListener("click", () => {
        onConfirm();
        bootstrap.Modal.getInstance(modalEl).hide();
    });

    // Show modal
    const bsModal = new bootstrap.Modal(modalEl);
    bsModal.show();
};
