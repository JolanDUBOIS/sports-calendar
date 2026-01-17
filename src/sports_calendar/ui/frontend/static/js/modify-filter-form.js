function openModifyFilterForm(filterId) {
    console.log("Opening modify filter form for filterId:", filterId);
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

    // show modal
    new bootstrap.Modal(
        document.getElementById("modifyFilterModal")
    ).show();
}