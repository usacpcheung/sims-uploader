(function () {
  const banner = document.querySelector('.tagline');
  if (!banner) {
    return;
  }

  const now = new Date();
  const formatter = new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short'
  });
  banner.dataset.renderedAt = formatter.format(now);
})();
