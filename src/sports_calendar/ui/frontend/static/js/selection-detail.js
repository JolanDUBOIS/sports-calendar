document.addEventListener("DOMContentLoaded", () => {

    const container = document.getElementById("items-list");
    if (!container) return;

    container.addEventListener("click", (event) => {
        const btn = event.target.closest("button[data-action='toggle-item']");
        if (!btn) return;

        const card = btn.closest(".card");
        if (!card) return;

        const details = card.querySelector(".item-details");
        if (!details) return;

        // Toggle visibility
        details.classList.toggle("d-none");

        // Optional: change button symbol
        btn.textContent = details.classList.contains("d-none") ? "+" : "−";
    });

});
