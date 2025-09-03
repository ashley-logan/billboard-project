import { themeQuartz } from 'ag-grid-community';

// to use myTheme in an application, pass it to the theme grid option
const myTheme = themeQuartz
	.withParams({
        accentColor: "#0006FF",
        backgroundColor: "#21222C",
        borderColor: "#0006FF",
        borderRadius: 0,
        browserColorScheme: "dark",
        cellHorizontalPaddingScale: 0.8,
        cellTextColor: "#FFFFFF",
        columnBorder: false,
        fontFamily: {
            googleFont: "Pixelify Sans"
        },
        fontSize: 14,
        foregroundColor: "#FFFFFF",
        headerBackgroundColor: "#21222C",
        headerFontSize: 14,
        headerFontWeight: 700,
        headerRowBorder: false,
        headerTextColor: "#FFFFFF",
        headerVerticalPaddingScale: 1.5,
        oddRowBackgroundColor: "#323343",
        rangeSelectionBackgroundColor: "#78779366",
        rangeSelectionBorderColor: "#FFFFFF",
        rangeSelectionBorderStyle: "solid",
        rowBorder: true,
        rowVerticalPaddingScale: 1.5,
        sidePanelBorder: false,
        spacing: 4,
        wrapperBorder: true,
        wrapperBorderRadius: 0
    });
