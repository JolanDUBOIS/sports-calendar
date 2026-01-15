// Delete selection

function deleteSelectionAPI(selectionId) {
    return fetch(`/selections/${selectionId}`, { method: 'DELETE' });
}

window.deleteSelection = function(selectionId, cardEl, event) {
    // Prevent the link from being followed
    if (event) {
        event.preventDefault();
        event.stopPropagation();
    }

    deleteSelectionAPI(selectionId)
        .then(response => {
            if (response.status === 204) {
                cardEl.remove();
                console.log(`Selection ${selectionId} deleted successfully.`);
            } else if (response.status === 404) {
                console.warn("Selection not found:", selectionId);
            } else {
                console.error("Server error while deleting selection:", response.status);
            }
        })
        .catch(err => console.error("Network error:", err));
}

// Create selection

function createSelectionAPI(name) {
    return fetch(`/selections/`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name })
    }).then(res => res.json());
}

window.createSelection = function(name, listContainer) {
    if (!name) {
        name = prompt("Enter a name for the new selection:");
        if (!name) return; // user cancelled
    }

    console.log("Creating selection with name:", name);
}

// Open create selection modal

const modal = document.getElementById("create-selection-modal");
const input = document.getElementById("new-selection-name");
const confirmBtn = document.getElementById("confirm-selection");
const cancelBtn = document.getElementById("cancel-selection");
const errorMsg = document.getElementById("selection-error-msg");

errorMsg.style.color = "red";
errorMsg.style.fontSize = "0.9em";
errorMsg.style.display = "none"; // hide by default

let lastValidation = null;

window.openCreateSelectionModal = function(onConfirmCallback) {
    // Reset modal state
    input.value = "";
    confirmBtn.disabled = true;
    errorMsg.style.display = "none";
    modal.classList.remove("hidden");
    input.focus();

    // --- Event listeners ---

    // Confirm handler
    const handleConfirm = () => {
        if (confirmBtn.disabled) return; // prevents invalid submission
        const name = input.value.trim();
        if (name) onConfirmCallback(name);
        closeModal();
    };

    // Cancel handler
    const handleCancel = () => closeModal();

    // Close modal function
    function closeModal() {
        modal.classList.add("hidden");

        // Remove listeners to prevent duplicates
        confirmBtn.removeEventListener("click", handleConfirm);
        cancelBtn.removeEventListener("click", handleCancel);
        modal.removeEventListener("click", handleOutsideClick);
        input.removeEventListener("input", handleInput);
        input.removeEventListener("keydown", handleKeyDown);
    }

    // Click outside modal closes it
    function handleOutsideClick(e) {
        if (e.target === modal) closeModal();
    }

    // Enter key triggers confirm
    function handleKeyDown(e) {
        if (e.key === "Enter") {
            handleConfirm();
        }
    }

    // Live validation handler
    async function handleInput() {
        const name = input.value.trim();

        if (!name) {
            errorMsg.textContent = "Name cannot be empty.";
            errorMsg.style.display = "block";
            confirmBtn.disabled = true;
            return;
        }

        // Cancel previous fetch if any
        if (lastValidation) lastValidation.abort();
        const controller = new AbortController();
        lastValidation = controller;

        try {
            const res = await fetch("/selections/validate-name", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ name }),
                signal: controller.signal
            });
            const data = await res.json();

            if (!data.valid) {
                errorMsg.textContent = data.error;
                errorMsg.style.display = "block";
                confirmBtn.disabled = true;
            } else {
                errorMsg.style.display = "none";
                confirmBtn.disabled = false;
            }
        } catch (err) {
            if (err.name !== "AbortError") console.error("Validation error:", err);
        }
    }

    // --- Attach listeners ---
    confirmBtn.addEventListener("click", handleConfirm);
    cancelBtn.addEventListener("click", handleCancel);
    modal.addEventListener("click", handleOutsideClick);
    input.addEventListener("input", handleInput);
    input.addEventListener("keydown", handleKeyDown);
};
