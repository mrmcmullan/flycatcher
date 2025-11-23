// Hide doctest markers (>>> and ...) from code blocks in documentation
// This makes examples cleaner while keeping them testable with doctest/xdoctest

document.addEventListener('DOMContentLoaded', function() {
    // Find all code blocks
    const codeBlocks = document.querySelectorAll('pre code, .highlight code');

    codeBlocks.forEach(function(codeBlock) {
        // Get the text content
        let text = codeBlock.textContent || codeBlock.innerText;

        // Remove lines starting with >>> or ... (doctest markers)
        // But preserve the indentation and content
        const lines = text.split('\n');
        const cleanedLines = lines.map(line => {
            // Remove >>> at start of line (with optional spaces before)
            line = line.replace(/^\s*>>>\s?/, '');
            // Remove ... at start of line (with optional spaces before)
            line = line.replace(/^\s*\.\.\.\s?/, '');
            return line;
        });

        // Only update if we actually found and removed markers
        const cleanedText = cleanedLines.join('\n');
        if (cleanedText !== text) {
            codeBlock.textContent = cleanedText;
        }
    });
});

