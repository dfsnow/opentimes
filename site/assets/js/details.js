/* eslint-disable no-undef */
const openDetailsOnLoad = () => {
  const target = window.location.hash,
    // eslint-disable-next-line sort-vars
    elTarget = document.querySelector(target);

  if (!elTarget) { return; }
  let elDetails = elTarget.closest("details");
  while (elDetails) {
    if (elDetails.matches("details")) { elDetails.open = true; }
    elDetails = elDetails.parentElement;
  }

  // Scroll to the target element
  elTarget.scrollIntoView();
};

window.addEventListener("load", openDetailsOnLoad);
