document.addEventListener("DOMContentLoaded", () => {

    const container = document.getElementById("selections-list");
    if (!container) return;

    // --------------------
    // Card click (except delete buttons)
    // --------------------
    container.addEventListener("click", (event) => {
        // Ignore clicks on buttons inside the card
        if (event.target.tagName === "BUTTON") return;

        // Find the closest clickable card
        const card = event.target.closest(".clickable");
        if (!card) return;

        const type = card.dataset.type;
        const name = card.dataset.name;

        handleCardClick(type, name);
    });

    function handleCardClick(type, name) {
        console.log("Card clicked:", type, name);
        if (type === "selection") {
            window.location.href = `/selections/${name}`;
        } else {
            console.error("Unknown card type:", type);
        }
    }

    // --------------------
    // Delete button click
    // --------------------
    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button[data-action='delete-selection']");
        if (!btn) return;

        const card = btn.closest(".card");
        if (!card) return;

        const type = card.dataset.type;
        const name = card.dataset.name;

        handleDeleteClick(type, name, card, event);
    });

    function handleDeleteClick(type, name, cardEl, event) {
        event.stopPropagation(); // prevent triggering card click
        console.log("Delete clicked:", type, name);

        const nameText = cardEl.querySelector(".fw-bold").textContent;
        const message = `Are you sure you want to delete ${nameText}?`;

        openDeleteModal(message, () => {
            fetch(`/selections/${name}`, { method: 'DELETE' })
                .then(res => {
                    if (res.ok) {
                        cardEl.remove();
                        console.log("Deleted:", type, name);
                    } else {
                        console.error("Delete failed:", res.status);
                    }
                })
                .catch(err => console.error("Delete error:", err));
        });
    }

    // --------------------
    // Create button click
    // --------------------
    const createBtn = container.querySelector("[data-action='create-selection']");
    if (createBtn) {
        createBtn.addEventListener("click", () => {
            console.log("Create selection button clicked");
            openCreateSelectionModal((name) => {
                if (!name) return;
                fetch('/selections/', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ name })
                })
                    .then(res => {
                        if (res.ok) {
                            // Optionally reload or update the list
                            window.location.reload();
                        } else {
                            console.error("Create failed:", res.status);
                        }
                    })
                    .catch(err => console.error("Create error:", err));
            });
        });
    }

    // ---------------------
    // Create Modal function
    // ---------------------
    function openCreateSelectionModal(onConfirm) {
        const modalEl = document.getElementById("createSelectionModal");
        const inputEl = modalEl.querySelector("#create-selection-name");
        const confirmBtn = modalEl.querySelector("#create-selection-confirm-btn");

        // Clear previous value
        inputEl.value = "";

        // Remove previous listeners
        confirmBtn.replaceWith(confirmBtn.cloneNode(true));
        const newConfirmBtn = modalEl.querySelector("#create-selection-confirm-btn");

        // Attach new listener
        newConfirmBtn.addEventListener("click", () => {
            const name = inputEl.value.trim();
            if (!name) return;
            onConfirm(name);
            bootstrap.Modal.getInstance(modalEl).hide();
        });

        // Show modal
        const bsModal = new bootstrap.Modal(modalEl);
        bsModal.show();

        // Focus input when modal is shown
        modalEl.addEventListener('shown.bs.modal', function handler() {
            inputEl.focus();
            modalEl.removeEventListener('shown.bs.modal', handler);
        });
    }
});
