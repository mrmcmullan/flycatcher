// Hide doctest markers (>>> and ...) from code blocks in documentation
// This makes examples cleaner while keeping them testable with doctest/xdoctest
// Works for both regular markdown code blocks and mkdocstrings-generated code blocks

function cleanDoctestMarkers(codeBlock) {
    // Get the text content
    let text = codeBlock.textContent || codeBlock.innerText;

    // Remove lines starting with >>>, <<<, or ... (doctest markers)
    // But preserve the indentation and content
    const lines = text.split('\n');
    const cleanedLines = lines.map(line => {
        // Remove >>> at start of line (with optional spaces before)
        line = line.replace(/^\s*>>>\s?/, '');
        // Remove <<< at start of line (with optional spaces before)
        line = line.replace(/^\s*<<<\s?/, '');
        // Remove ... at start of line (with optional spaces before)
        line = line.replace(/^\s*\.\.\.\s?/, '');
        return line;
    });

    // Only update if we actually found and removed markers
    const cleanedText = cleanedLines.join('\n');
    if (cleanedText !== text) {
        codeBlock.textContent = cleanedText;
    }
}

function processAllCodeBlocks() {
    // Find all code blocks including those in mkdocstrings (.doc containers)
    const codeBlocks = document.querySelectorAll(
        'pre code, .highlight code, .doc pre code, .doc .highlight code, ' +
        '.doc-contents pre code, .doc-contents .highlight code, ' +
        '.mkdocstrings-source pre code, .mkdocstrings-source .highlight code'
    );

    codeBlocks.forEach(cleanDoctestMarkers);
}

// Process code blocks when DOM is ready
document.addEventListener('DOMContentLoaded', processAllCodeBlocks);

// Also process code blocks that might be added dynamically by mkdocstrings
// Use MutationObserver to catch dynamically loaded content
if (typeof MutationObserver !== 'undefined') {
    const observer = new MutationObserver(function(mutations) {
        let shouldProcess = false;
        mutations.forEach(function(mutation) {
            if (mutation.addedNodes.length > 0) {
                // Check if any added nodes contain code blocks
                mutation.addedNodes.forEach(function(node) {
                    if (node.nodeType === 1) { // Element node
                        if (node.tagName === 'CODE' ||
                            node.querySelector && node.querySelector('code')) {
                            shouldProcess = true;
                        }
                    }
                });
            }
        });
        if (shouldProcess) {
            processAllCodeBlocks();
        }
    });

    // Start observing when DOM is ready
    document.addEventListener('DOMContentLoaded', function() {
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    });
}

